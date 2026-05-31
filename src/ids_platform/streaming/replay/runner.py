from __future__ import annotations

import random
import time

from ids_platform.streaming.replay.config import ReplayConfig
from ids_platform.streaming.replay.perturbation import ReplayPerturbation
from ids_platform.streaming.replay.publisher import ReplayRecordBuilder, ReplayPublisher
from ids_platform.streaming.replay.rate_limiter import sleep_for_rate_limit


def run_replay_job(config: ReplayConfig) -> int:
    started_at = time.perf_counter()

    publisher = ReplayPublisher(config.runtime)
    record_builder = ReplayRecordBuilder(config.runtime.run_tag)
    perturbation = ReplayPerturbation(config.timing)
    rng = random.Random(int(config.timing.random_seed))

    batches = config.source.table.to_batches(config.source.batch_size)
    sent_rows = 0

    for chunk_index, batch in enumerate(batches, start=1):
        chunk = batch.to_pandas()
        chunk = perturbation.apply(chunk, chunk_index=chunk_index, rng=rng)

        for offset, (_, row) in enumerate(chunk.iterrows()):
            row_index = sent_rows + offset
            record = record_builder.build_replay_record(row, row_index=row_index)
            publisher.publish_record(record)

        publisher.flush_or_raise("publishing attempt", chunk_index=chunk_index)

        sent_rows += len(chunk)
        sleep_for_rate_limit(
            rate=config.rate,
            sent_rows=sent_rows,
            started=started_at
        )

    sentinel_record = record_builder.build_input_sentinel_record()
    publisher.publish_sentinel(sentinel_record)

    return sent_rows
