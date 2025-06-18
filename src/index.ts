import {FastifyInstance, FastifyRequest} from 'fastify';
import {HyperionDelta, HyperionDeltaHandler, HyperionPlugin} from '@eosrio/hyperion-plugin-core';

// Extended FastifyInstance interface for proper typing
interface ExtendedFastifyInstance extends FastifyInstance {
    elastic: any;
    manager: {
        chain: string;
        config: any;
    };
}

export interface DelphioracleConfig {
    table?: string;
    contract?: string;
}

interface OracleDatapointsQuery {
    scope?: string;
    interval?: string;
    after?: string;
    before?: string;
    size?: number;
}

// Valid Elasticsearch calendar intervals (fixed calendar units)
const CALENDAR_INTERVALS = ['1m', '1h', '1d', '1w', '1M', '1q', '1y'];

// Valid Elasticsearch fixed intervals (can be multiples)
const FIXED_INTERVALS = [
    '1m',
    '2m',
    '5m',
    '10m',
    '15m',
    '20m',
    '30m',
    '1h',
    '2h',
    '3h',
    '4h',
    '5h',
    '6h',
    '8h',
    '12h',
    '1d',
    '2d',
    '3d',
    '7d',
    '1w',
    '2w'
];

// Function to validate interval and determine type
function getIntervalConfig(interval: string): {interval: string; type: 'calendar' | 'fixed'} {
    // Default fallback
    if (!interval) {
        return {interval: '1h', type: 'calendar'};
    }

    // Check if it's a valid calendar interval
    if (CALENDAR_INTERVALS.includes(interval)) {
        return {interval, type: 'calendar'};
    }

    // Check if it's a valid fixed interval
    if (FIXED_INTERVALS.includes(interval)) {
        return {interval, type: 'fixed'};
    }

    // Default fallback for invalid intervals
    return {interval: '1h', type: 'calendar'};
}

export default class DelphioraclePlugin extends HyperionPlugin {
    apiPlugin = true;
    hasApiRoutes = true;
    deltaHandlers: HyperionDeltaHandler[] = [];

    addRoutes(server: FastifyInstance): void {
        // Original route
        server.get('/v2/history/get_oracle_datapoints', async (request, reply) => {
            reply.send({message: 'Delphioracle API is running!'});
        });

        // New route for oracle datapoints histogram
        server.get('/v2/oracle/get_datapoints_histogram', async (request: FastifyRequest<{Querystring: OracleDatapointsQuery}>, reply) => {
            try {
                const extServer = server as ExtendedFastifyInstance;
                const query = request.query;

                console.log();
                console.log();
                console.log();
                console.log();
                console.log();
                console.log();
                console.log(`query`, query);
                const contractName = this.baseConfig?.contract || 'delphioracle';
                const tableName = this.baseConfig?.table || 'datapoints';

                // Default values
                const scope = query.scope || 'tlosusd';
                const intervalConfig = getIntervalConfig(query.interval || '1h');
                const size = query.size || 0;

                // Build time range
                const timeRange: any = {};
                if (query.after) {
                    timeRange.gte = new Date(query.after).toISOString();
                } else {
                    // Default to last 24 hours
                    timeRange.gte = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
                }

                if (query.before) {
                    timeRange.lte = new Date(query.before).toISOString();
                } else {
                    timeRange.lte = new Date().toISOString();
                }

                // Build Elasticsearch query following the provided model
                const dateHistogramConfig: any = {
                    field: '@timestamp'
                };

                // Use the appropriate interval type
                if (intervalConfig.type === 'calendar') {
                    dateHistogramConfig.calendar_interval = intervalConfig.interval;
                } else {
                    dateHistogramConfig.fixed_interval = intervalConfig.interval;
                }

                const searchBody = {
                    aggs: {
                        histogram: {
                            date_histogram: dateHistogramConfig,
                            aggs: {
                                average_price: {
                                    avg: {
                                        field: '@datapoints.value'
                                    }
                                },
                                min_price: {
                                    min: {
                                        field: '@datapoints.value'
                                    }
                                },
                                max_price: {
                                    max: {
                                        field: '@datapoints.value'
                                    }
                                },
                                median_price: {
                                    avg: {
                                        field: '@datapoints.median'
                                    }
                                }
                            }
                        }
                    },
                    size: size,
                    query: {
                        bool: {
                            must: [
                                {
                                    range: {
                                        '@timestamp': {
                                            format: 'strict_date_optional_time',
                                            gte: timeRange.gte,
                                            lte: timeRange.lte
                                        }
                                    }
                                },
                                {
                                    term: {
                                        code: {
                                            value: contractName
                                        }
                                    }
                                },
                                {
                                    term: {
                                        table: {
                                            value: tableName
                                        }
                                    }
                                },
                                {
                                    term: {
                                        scope: {
                                            value: scope
                                        }
                                    }
                                }
                            ]
                        }
                    }
                };

                // Execute Elasticsearch query
                const results = await extServer.elastic.search({
                    index: extServer.manager.chain + '-delta-*',
                    ...searchBody
                });

                // Process results
                const response = {
                    query_time_ms: 0, // Will be filled by timing wrapper if used
                    scope: scope,
                    contract: contractName,
                    table: tableName,
                    interval: intervalConfig.interval,
                    time_range: {
                        from: timeRange.gte,
                        to: timeRange.lte
                    },
                    total_documents: results.hits?.total || 0,
                    histogram: []
                };

                // Extract histogram buckets
                if (results.aggregations?.histogram?.buckets) {
                    response.histogram = results.aggregations.histogram.buckets.map((bucket: any) => ({
                        timestamp: bucket.key_as_string || bucket.key,
                        doc_count: bucket.doc_count,
                        average_price: bucket.average_price?.value || null,
                        min_price: bucket.min_price?.value || null,
                        max_price: bucket.max_price?.value || null,
                        median_price: bucket.median_price?.value || null
                    }));
                }

                reply.send(response);
            } catch (error: any) {
                server.log.error('Error in get_datapoints_histogram:', error);
                reply.status(500).send({
                    error: 'Internal server error',
                    message: error.message
                });
            }
        });
    }

    constructor(config: DelphioracleConfig) {
        super(config);
        console.log('DelphioraclePlugin initialized with config:', config);
        const tableName = config.table || 'datapoints';

        const mappings = {
            delta: {}
        };

        mappings.delta['@' + tableName] = {
            properties: {
                value: {type: 'long'},
                owner: {type: 'keyword'},
                median: {type: 'long'}
            }
        };

        this.deltaHandlers.push({
            table: tableName,
            contract: config.contract || 'delphioracle',
            mappings,
            handler: async (delta: HyperionDelta) => {
                const data = delta['data'];
                if (data['value'] && data['median'] && data['owner']) {
                    delta['@' + tableName] = {
                        owner: data['owner'],
                        value: data['value'],
                        median: data['median']
                    };
                    delete delta['data'];
                }
                // console.log(delta);
            }
        });
    }
}
