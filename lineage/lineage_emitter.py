import datahub.emitter.mce_builder as builder
import os
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import ChangeTypeClass


def emit_table_to_table(up_streams, down_stream):
    # 构建上游列表
    up_stream_tables = []
    for up_stream in up_streams:
        up_stream_tables.append(UpstreamClass(
            dataset=builder.make_dataset_urn("clickhouse", "hho_hz." + up_stream, "PROD"),
            type=DatasetLineageTypeClass.TRANSFORMED,
        ))
    # 构建血缘对象
    up_stream_lineage = UpstreamLineage(upstreams=up_stream_tables)
    # 构建MetadataChangeProposalWrapper
    lineage_mcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=builder.make_dataset_urn("clickhouse", "hho_hz." + down_stream, "PROD"),
        aspectName="upstreamLineage",
        aspect=up_stream_lineage,
    )
    # 构建发送器
    datahub_host = os.getenv('datahub_host')
    datahub_port = os.getenv('datahub_port')
    emitter = DatahubRestEmitter(f'http://{datahub_host}:{datahub_port}')

    # 发送
    emitter.emit_mcp(lineage_mcp)
