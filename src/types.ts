/**
 * Type definitions for the LLM chat application.
 */

export interface Env {
	/**
	 * Binding for the Workers AI API.
	 */
	AI: Ai;

	/**
	 * Binding for static assets.
	 */
	ASSETS: { fetch: (request: Request) => Promise<Response> };

	/**
	 * Optional AI Gateway ID. When unset, requests go straight to Workers AI.
	 */
	AI_GATEWAY_ID?: string;

	/**
	 * Optional: set to "true" to bypass the AI Gateway cache.
	 */
	AI_GATEWAY_SKIP_CACHE?: string;

	/**
	 * Optional: AI Gateway cache time-to-live in seconds.
	 */
	AI_GATEWAY_CACHE_TTL?: string;
}

/**
 * Represents a chat message.
 */
export interface ChatMessage {
	role: "system" | "user" | "assistant";
	content: string;
}
