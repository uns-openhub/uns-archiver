/* Auto-generated. Do not edit by hand. */
export interface ProjectAppConfig {
    uns: {
        graphql: string;
        rest: string;
        /** Bearer token used for service-to-service access to the UNS instance. */
        token?: string | undefined;
        /** Email used when authenticating to graphql endpoint of the UNS instance. */
        email?: string | undefined;
        /** Password or secret value paired with the UNS email. */
        password?: string | undefined;
        instanceMode?: "wait" | "force" | "handover";
        /** Process name used in MQTT topics and logs. */
        processName: string;
        handover?: boolean;
        /** Optional PM2/controller supervisor guard settings for this RTT instance. */
        supervisor?: {
            /** Enable controller/PM2 supervisor handling for this RTT instance. */
            enabled?: boolean;
            /** Let PM2 restart the process when it exits unexpectedly. */
            restartOnExit?: boolean;
            /** Optional PM2 memory restart limit in megabytes. */
            maxMemoryMb?: number | undefined;
            /** Let the controller auto-start this instance when required system-service runtime signals are absent. */
            restartOnUnhealthy?: boolean;
            /** How long runtime signals must stay unhealthy before the controller supervisor can act. */
            unhealthyAfterMs?: number;
            /** Minimum time between controller supervisor restart attempts for this instance. */
            restartCooldownMs?: number;
        } | undefined;
        jwksWellKnownUrl?: string | undefined;
        kidWellKnownUrl?: string | undefined;
        env?: "dev" | "staging" | "test" | "prod";
    };
    logging?: {
        adapter?: string;
        host: string;
        port?: number;
        level?: "error" | "warn" | "info" | "http" | "verbose" | "debug" | "silly";
    } | undefined;
    input?: {
        host?: string | undefined;
        hosts?: string[] | undefined;
        servers?: {
            host: string;
            port?: number | undefined;
            protocol?: ("mqtt" | "mqtts" | "ws" | "wss" | "tcp" | "ssl") | undefined;
        }[] | undefined;
        port?: number | undefined;
        protocol?: ("mqtt" | "mqtts" | "ws" | "wss" | "tcp" | "ssl") | undefined;
        username?: string | undefined;
        password?: string | undefined;
        clientId?: string | undefined;
        clean?: boolean | undefined;
        keepalive?: number | undefined;
        connectTimeout?: number | undefined;
        reconnectPeriod?: number | undefined;
        reconnectOnConnackError?: boolean | undefined;
        resubscribe?: boolean | undefined;
        queueQoSZero?: boolean | undefined;
        rejectUnauthorized?: boolean | undefined;
        properties?: {
            sessionExpiryInterval?: number | undefined;
            receiveMaximum?: number | undefined;
            maximumPacketSize?: number | undefined;
            topicAliasMaximum?: number | undefined;
            requestResponseInformation?: boolean | undefined;
            requestProblemInformation?: boolean | undefined;
            userProperties?: {
                [x: string]: string;
            } | undefined;
        } | undefined;
        ca?: string | undefined;
        cert?: string | undefined;
        key?: string | undefined;
        servername?: string | undefined;
    } | undefined;
    output?: {
        host?: string | undefined;
        hosts?: string[] | undefined;
        servers?: {
            host: string;
            port?: number | undefined;
            protocol?: ("mqtt" | "mqtts" | "ws" | "wss" | "tcp" | "ssl") | undefined;
        }[] | undefined;
        port?: number | undefined;
        protocol?: ("mqtt" | "mqtts" | "ws" | "wss" | "tcp" | "ssl") | undefined;
        username?: string | undefined;
        password?: string | undefined;
        clientId?: string | undefined;
        clean?: boolean | undefined;
        keepalive?: number | undefined;
        connectTimeout?: number | undefined;
        reconnectPeriod?: number | undefined;
        reconnectOnConnackError?: boolean | undefined;
        resubscribe?: boolean | undefined;
        queueQoSZero?: boolean | undefined;
        rejectUnauthorized?: boolean | undefined;
        properties?: {
            sessionExpiryInterval?: number | undefined;
            receiveMaximum?: number | undefined;
            maximumPacketSize?: number | undefined;
            topicAliasMaximum?: number | undefined;
            requestResponseInformation?: boolean | undefined;
            requestProblemInformation?: boolean | undefined;
            userProperties?: {
                [x: string]: string;
            } | undefined;
        } | undefined;
        ca?: string | undefined;
        cert?: string | undefined;
        key?: string | undefined;
        servername?: string | undefined;
    } | undefined;
    infra: {
        host?: string | undefined;
        hosts?: string[] | undefined;
        servers?: {
            host: string;
            port?: number | undefined;
            protocol?: ("mqtt" | "mqtts" | "ws" | "wss" | "tcp" | "ssl") | undefined;
        }[] | undefined;
        port?: number | undefined;
        protocol?: ("mqtt" | "mqtts" | "ws" | "wss" | "tcp" | "ssl") | undefined;
        username?: string | undefined;
        password?: string | undefined;
        clientId?: string | undefined;
        clean?: boolean | undefined;
        keepalive?: number | undefined;
        connectTimeout?: number | undefined;
        reconnectPeriod?: number | undefined;
        reconnectOnConnackError?: boolean | undefined;
        resubscribe?: boolean | undefined;
        queueQoSZero?: boolean | undefined;
        rejectUnauthorized?: boolean | undefined;
        properties?: {
            sessionExpiryInterval?: number | undefined;
            receiveMaximum?: number | undefined;
            maximumPacketSize?: number | undefined;
            topicAliasMaximum?: number | undefined;
            requestResponseInformation?: boolean | undefined;
            requestProblemInformation?: boolean | undefined;
            userProperties?: {
                [x: string]: string;
            } | undefined;
        } | undefined;
        ca?: string | undefined;
        cert?: string | undefined;
        key?: string | undefined;
        servername?: string | undefined;
    };
    devops?: {
        provider?: "azure-devops";
        organization: string;
        project?: string | undefined;
    } | undefined;
    /** Archiver runtime settings. */
    archiver?: {
        /** Max number of events kept in-memory while waiting for the controller/GraphQL active-topics registry (default 2000). Overflow spills to ./event_storage. */
        inactiveBufferMax?: number | undefined;
        /** Max age (ms) for buffered inactive-topic events before spilling to ./event_storage (default 300000 = 5min). */
        inactiveBufferMaxAgeMs?: number | undefined;
        /** Enable ingest/buffer trace logs. Equivalent to setting UNS_ARCHIVER_TRACE=1 or UNS_ARCHIVER_TRACE_INGEST=1. */
        traceIngest?: boolean | undefined;
    } | undefined;
    questdb: {
        /** Legacy QuestDB ILP connection string. Prefer url, username, and password for production secrets. */
        configurationString?: string | undefined;
        /** QuestDB HTTP endpoint used with the structured credential form (for example https://questdb.example:9000). */
        url?: string | undefined;
        /** QuestDB username used with questdb.url. Store as a secret reference in production. */
        username?: string | undefined;
        /** QuestDB password used with questdb.url. Store as a secret reference in production. */
        password?: string | undefined;
        dataStorage: {
            /** Prefix used when naming QuestDB tables for this topic */
            tablePrefix: string;
            /** UNS topic filter subscribed for ingestion */
            topic: string;
            /** Default ingestion mode: append (default), dedup (upsert on eventId), or window_replace. Note: QuestDB has no row-level DELETE; window_replace is implemented as soft delete (sets deleted=true for rows in the window that were not refreshed). */
            ingestMode?: ("append" | "dedup" | "window_replace") | undefined;
            /** Optional override of ingestMode for data messages */
            ingestModeData?: ("append" | "dedup" | "window_replace") | undefined;
            /** Optional override of ingestMode for table messages */
            ingestModeTable?: ("append" | "dedup" | "window_replace") | undefined;
            /** Per-dataGroup ingestion mode overrides */
            dataGroups?: {
                name: string;
                /** Ingestion mode override for this dataGroup */
                ingestMode: "append" | "dedup" | "window_replace";
                /** Optional override for data messages in this dataGroup */
                ingestModeData?: ("append" | "dedup" | "window_replace") | undefined;
                /** Optional override for table messages in this dataGroup */
                ingestModeTable?: ("append" | "dedup" | "window_replace") | undefined;
            }[] | undefined;
        }[];
    };
}

export interface AppConfig extends ProjectAppConfig {}

type GeneratedProjectAppConfig = ProjectAppConfig;
type GeneratedAppConfig = AppConfig;

declare module "@uns-kit/core/config/app-config.js" {
  interface ProjectAppConfig extends GeneratedProjectAppConfig {}
  interface AppConfig extends GeneratedAppConfig {}
}
