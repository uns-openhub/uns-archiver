type ControllerEndpointConfig = {
  rest?: unknown;
  graphql?: unknown;
};

type ControllerEndpointEnvironment = Partial<
  Pick<NodeJS.ProcessEnv, "UNS_CONTROLLER_PUBLIC_BASE">
>;

function nonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function managedControllerBase(environment: ControllerEndpointEnvironment): string | null {
  const raw = nonEmptyString(environment.UNS_CONTROLLER_PUBLIC_BASE);
  if (!raw) return null;
  try {
    const url = new URL(raw);
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    return raw.replace(/\/+$/, "");
  } catch {
    return null;
  }
}

/**
 * Controller-managed RTT processes must call the controller that launched them.
 * Static config remains the fallback for direct repository development.
 */
export function resolveControllerEndpoints(
  config: ControllerEndpointConfig,
  environment: ControllerEndpointEnvironment = process.env,
): { rest: string; graphql: string; managed: boolean } {
  const base = managedControllerBase(environment);
  if (base) {
    return {
      rest: `${base}/api`,
      graphql: `${base}/graphql`,
      managed: true,
    };
  }
  return {
    rest: nonEmptyString(config.rest) ?? "",
    graphql: nonEmptyString(config.graphql) ?? "",
    managed: false,
  };
}
