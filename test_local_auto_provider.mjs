import assert from "node:assert/strict";
import test from "node:test";

import localAutoProvider, { modelConfigFromCatalog } from "./extensions/local-auto-provider.ts";

test("maps DeepSeek broker metadata to a stable reasoning model", () => {
  const model = modelConfigFromCatalog({
    id: "local-auto",
    root: "deepseek-ai/DeepSeek-V4-Flash-0731",
    max_model_len: 524288,
  });

  assert.equal(model.id, "local-auto");
  assert.equal(model.contextWindow, 524288);
  assert.equal(model.maxTokens, 65536);
  assert.equal(model.reasoning, true);
  assert.equal(model.compat.thinkingFormat, "deepseek");
});

test("maps Qwen broker metadata to qwen chat-template thinking", () => {
  const model = modelConfigFromCatalog({
    id: "local-auto",
    root: "unsloth/Qwen3.6-35B-A3B-NVFP4",
    max_model_len: 262144,
  });

  assert.equal(model.contextWindow, 262144);
  assert.equal(model.reasoning, true);
  assert.equal(model.compat.thinkingFormat, "qwen-chat-template");
});

test("honors the broker output limit", () => {
  const model = modelConfigFromCatalog({
    id: "local-auto",
    root: "vendor/smaller-model",
    max_model_len: 262144,
    max_output_tokens: 8192,
  });

  assert.equal(model.maxTokens, 8192);
});

test("uses conservative defaults for an unknown local model", () => {
  const model = modelConfigFromCatalog({ id: "local-auto", root: "vendor/plain-model" });

  assert.equal(model.contextWindow, 16384);
  assert.equal(model.maxTokens, 4096);
  assert.equal(model.reasoning, false);
  assert.equal(model.compat.thinkingFormat, undefined);
});

test("refreshes and rebinds an active local-auto model before each run", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () =>
    new Response(
      JSON.stringify({
        data: [
          {
            id: "local-auto",
            root: "deepseek-ai/DeepSeek-V4-Flash-0731",
            max_model_len: 524288,
          },
        ],
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );

  let beforeAgentStart;
  let selectedModel;
  const pi = {
    registerProvider() {},
    on(event, handler) {
      if (event === "before_agent_start") beforeAgentStart = handler;
    },
    async setModel(model) {
      selectedModel = model;
      return true;
    },
  };

  try {
    await localAutoProvider(pi);
    const refreshed = modelConfigFromCatalog({
      id: "local-auto",
      root: "unsloth/Qwen3.6-35B-A3B-NVFP4",
      max_model_len: 262144,
    });
    let refreshCalls = 0;
    await beforeAgentStart({}, {
      model: { provider: "local-auto", id: "local-auto" },
      modelRegistry: {
        async refresh() {
          refreshCalls += 1;
          return { aborted: false, errors: new Map() };
        },
        find() {
          return refreshed;
        },
      },
    });

    assert.equal(refreshCalls, 1);
    assert.equal(selectedModel.contextWindow, 262144);
    assert.equal(selectedModel.compat.thinkingFormat, "qwen-chat-template");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("does not rebind when live metadata is unchanged", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () =>
    new Response(
      JSON.stringify({
        data: [
          {
            id: "local-auto",
            root: "deepseek-ai/DeepSeek-V4-Flash-0731",
            max_model_len: 524288,
          },
        ],
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );

  let beforeAgentStart;
  let setModelCalls = 0;
  const current = {
    provider: "local-auto",
    ...modelConfigFromCatalog({
      id: "local-auto",
      root: "deepseek-ai/DeepSeek-V4-Flash-0731",
      max_model_len: 524288,
    }),
  };
  const pi = {
    registerProvider() {},
    on(event, handler) {
      if (event === "before_agent_start") beforeAgentStart = handler;
    },
    async setModel() {
      setModelCalls += 1;
      return true;
    },
  };

  try {
    await localAutoProvider(pi);
    await beforeAgentStart({}, {
      model: current,
      modelRegistry: {
        async refresh() {
          return { aborted: false, errors: new Map() };
        },
        find() {
          return current;
        },
      },
    });
    assert.equal(setModelCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
