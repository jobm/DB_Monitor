/**
 * Minimal JavaScript SDK for the DB Monitor HTTP API.
 */
export class DBMonitorClient {
  /**
   * Create a DB Monitor client.
   * @param {object} options
   * @param {string} options.baseUrl
   * @param {string | null} [options.apiKey]
   * @param {string | null} [options.accessToken]
   */
  constructor({ baseUrl, apiKey = null, accessToken = null }) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.apiKey = apiKey;
    this.accessToken = accessToken;
  }

  /**
   * Exchange the configured API key for an access token.
   * @returns {Promise<string>}
   */
  async exchangeAccessToken() {
    if (!this.apiKey) {
      throw new Error("An API key is required to exchange a token.");
    }

    const payload = await this._requestJson("/auth/token", {
      method: "POST",
      headers: { "X-API-Key": this.apiKey },
    });
    this.accessToken = payload.access_token;
    return this.accessToken;
  }

  /**
   * Return basic API metadata.
   * @returns {Promise<object>}
   */
  getInfo() {
    return this._requestJson("/info");
  }

  /**
   * Return discovered tables.
   * @returns {Promise<object>}
   */
  getTables() {
    return this._requestJson("/tables");
  }

  /**
   * Return recent events with optional filters.
   * @param {object} [options]
   * @returns {Promise<object>}
   */
  getEvents(options = {}) {
    const { limit = 100, serviceName = null, operation = null } = options;
    const query = { limit: String(limit) };
    if (serviceName) {
      query.service_name = serviceName;
    }
    if (operation) {
      query.operation = operation;
    }
    return this._requestJson("/events", { query });
  }

  /**
   * Return recent column changes for one table or record.
   * @param {object} options
   * @returns {Promise<object>}
   */
  getChanges(options) {
    const {
      tableName,
      serviceName = null,
      rowIdentity = null,
      limit = 100,
      offset = 0,
    } = options;
    const query = {
      table_name: tableName,
      limit: String(limit),
      offset: String(offset),
    };
    if (serviceName) {
      query.service_name = serviceName;
    }
    if (rowIdentity) {
      query.row_identity = JSON.stringify(rowIdentity);
    }
    return this._requestJson("/changes", { query });
  }

  /**
   * Return consumer checkpoints for admin callers.
   * @returns {Promise<object>}
   */
  getConsumerCheckpoints() {
    return this._requestJson("/admin/checkpoints");
  }

  /**
   * Return dead-letter rows for admin callers.
   * @param {object} [options]
   * @returns {Promise<object>}
   */
  getDeadLetterEvents(options = {}) {
    const { limit = 100, includeReplayed = false } = options;
    return this._requestJson("/admin/dlq", {
      query: {
        limit: String(limit),
        include_replayed: String(includeReplayed),
      },
    });
  }

  /**
   * Send one request and parse the JSON body.
   * @param {string} path
   * @param {object} [options]
   * @returns {Promise<object>}
   */
  async _requestJson(path, options = {}) {
    const { method = "GET", query = null, headers = {} } = options;
    const requestHeaders = {
      ...this._defaultHeaders(),
      ...headers,
    };
    const url = new URL(`${this.baseUrl}${path}`);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        url.searchParams.set(key, value);
      }
    }

    const response = await fetch(url, {
      method,
      headers: requestHeaders,
    });
    if (!response.ok) {
      throw new Error(`DB Monitor request failed: ${response.status}`);
    }
    return await response.json();
  }

  /**
   * Return the best available authentication headers.
   * @returns {Record<string, string>}
   */
  _defaultHeaders() {
    if (this.accessToken) {
      return { Authorization: `Bearer ${this.accessToken}` };
    }
    if (this.apiKey) {
      return { "X-API-Key": this.apiKey };
    }
    return {};
  }
}