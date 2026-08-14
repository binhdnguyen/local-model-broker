import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

type CatalogModel = Record<string, unknown>;

type RefreshContext = {
  signal: AbortSignal;
};

const BROKER_BASE_URL = "http://127.0.0.1:8879/v1";
const PROVIDER_ID = "local-auto";

const ZERO_COST = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };

export function modelConfigFromCatalog(value: unknown) {
  const catalogModel = asRecord(value);
  const rootCandidate = catalogModel.root ?? catalogModel.id;
  const root =
    typeof rootCandidate === "string" && rootCandidate.length > 0
      ? rootCandidate
      : "local-auto";
  const lower = root.toLowerCase();
  const contextCandidate = Number(
    catalogModel.max_model_len ??
      catalogModel.context_window ??
      catalogModel.context_length ??
      catalogModel.max_context_length,
  );
  const contextWindow =
    Number.isInteger(contextCandidate) && contextCandidate > 0 ? contextCandidate : 16384;
  const outputCandidate = Number(catalogModel.max_output_tokens ?? catalogModel.max_tokens);
  const derivedMaxTokens = Math.min(65536, Math.max(4096, Math.floor(contextWindow / 4)));
  const maxTokens =
    Number.isInteger(outputCandidate) && outputCandidate > 0
      ? Math.min(65536, outputCandidate)
      : derivedMaxTokens;
  const qwen = lower.includes("qwen");
  const deepseek = lower.includes("deepseek");
  const reasoning = qwen || deepseek || lower.includes("reason") || lower.includes("thinking");

  return {
    id: "local-auto",
    name: `Local Auto — ${root}`,
    reasoning,
    input: ["text"],
    contextWindow,
    maxTokens,
    cost: ZERO_COST,
    compat: {
      supportsDeveloperRole: false,
      supportsReasoningEffort: false,
      maxTokensField: "max_tokens",
      thinkingFormat: qwen ? "qwen-chat-template" : deepseek ? "deepseek" : undefined,
    },
  };
}

function asRecord(value: unknown): CatalogModel {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Local model broker returned an invalid catalog entry");
  }
  return value as CatalogModel;
}

async function fetchModel(signal?: AbortSignal) {
  const response = await fetch(`${BROKER_BASE_URL}/models`, {
    headers: { accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Local model broker catalog failed: HTTP ${response.status}`);
  }
  const payload = asRecord(await response.json());
  if (!Array.isArray(payload.data) || payload.data.length === 0) {
    throw new Error("Local model broker returned an empty catalog");
  }
  return modelConfigFromCatalog(payload.data[0]);
}

export default async function localAutoProvider(pi: ExtensionAPI) {
  let initial;
  try {
    initial = await fetchModel(AbortSignal.timeout(3000));
  } catch {
    initial = modelConfigFromCatalog({ id: "local-auto", root: "local model unavailable" });
  }

  pi.registerProvider(PROVIDER_ID, {
    name: "Local Auto (:8888/:8880)",
    baseUrl: BROKER_BASE_URL,
    apiKey: "local",
    api: "openai-completions",
    models: [initial],
    async refreshModels({ signal }: RefreshContext) {
      return [await fetchModel(signal)];
    },
  });

  pi.on("before_agent_start", async (_event, ctx) => {
    if (ctx.model?.provider !== PROVIDER_ID || ctx.model.id !== "local-auto") {
      return;
    }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 3000);
    try {
      const result = await ctx.modelRegistry.refresh({
        providers: [PROVIDER_ID],
        signal: controller.signal,
      });
      if (result.aborted || result.errors.has(PROVIDER_ID)) {
        return;
      }
      const refreshed = ctx.modelRegistry.find(PROVIDER_ID, "local-auto");
      const metadataChanged =
        refreshed !== undefined &&
        (ctx.model.name !== refreshed.name ||
          ctx.model.contextWindow !== refreshed.contextWindow ||
          ctx.model.maxTokens !== refreshed.maxTokens ||
          ctx.model.reasoning !== refreshed.reasoning ||
          JSON.stringify(ctx.model.compat) !== JSON.stringify(refreshed.compat));
      if (refreshed && metadataChanged) {
        await pi.setModel(refreshed);
      }
    } finally {
      clearTimeout(timeout);
    }
  });
}
