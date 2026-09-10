#include <stddef.h>
#include <rdkafka.h>
#include <rdkafka_mock.h>

rd_kafka_mock_cluster_t * rd_kafka_mock_cluster_new(rd_kafka_t * client, int broker_count)
{
    (void)client;
    (void)broker_count;
    return NULL;
}

void rd_kafka_mock_cluster_destroy(rd_kafka_mock_cluster_t * cluster)
{
    (void)cluster;
}

const char * rd_kafka_mock_cluster_bootstraps(const rd_kafka_mock_cluster_t * cluster)
{
    (void)cluster;
    return NULL;
}

rd_kafka_resp_err_t rd_kafka_mock_broker_set_rtt(rd_kafka_mock_cluster_t * cluster, int32_t broker_id, int milliseconds)
{
    (void)cluster;
    (void)broker_id;
    (void)milliseconds;
    return RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED;
}

int unittest_mock_cluster(void);

int unittest_mock_cluster(void)
{
    return 1;
}