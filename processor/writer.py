import gzip
import os
import logging
import numpy as np
from reader import NWBElectricalSeriesReader
from utils import to_big_endian

log = logging.getLogger()

class TimeSeriesChunkWriter:
    """
    Attributes:
        output_dir (str): path to output directory for chunked sample data binary files
        chunk_size (int): number of samples (rounded down) to include in a single chunked sample data binary file (pre-compression)
            each sample is represented as a 64-bit (8 byte) floating-point value
    """

    def __init__(self, session_start_time, output_dir, chunk_size):
        self.session_start_time = session_start_time
        self.output_dir = output_dir
        self.chunk_size = chunk_size

    def write_electrical_series(self, electrical_series):
        """
        Chunks the sample data in two stages:
            1. Splits sample data into contiguous segments using the given or generated timestamp values
            2. Chunks each contiguous segment into the given chunk_size (number of samples to include per file)

        Writes each chunk to the given output directory
        """
        reader = NWBElectricalSeriesReader(electrical_series, self.session_start_time)

        for contiguous_start, contiguous_end in reader.contiguous_chunks():
            for chunk_start in range(contiguous_start, contiguous_end, self.chunk_size):
                chunk_end = min(contiguous_end, chunk_start + self.chunk_size)

                start_time = reader.timestamps[chunk_start]
                end_time = reader.timestamps[chunk_end-1]

                for channel_index in range(len(reader.channels)):
                    chunk = reader.get_chunk(channel_index, chunk_start, chunk_end)
                    channel = reader.channels[channel_index]
                    self.write_chunk(chunk, start_time, end_time, channel)

        for channel in reader.channels:
            channel.write(self.output_dir)

    def write_chunk(self, chunk, start_time, end_time, channel):
        """
        Formats the chunked sample data into 64-bit (8 byte) values in big-endian.

        Writes the chunked sample data to a gzipped binary file.
        """
        # ensure the samples are 64-bit float-pointing numbers in big-endian before converting to bytes
        formatted_data = to_big_endian(chunk.astype(np.float64)) 

        channel_index = '{index:05d}'.format(index=channel.index)
        file_name = "channel-{}_{}_{}.bfts.gz".format(channel_index, int(start_time * 1e6), int(end_time * 1e6))
        file_path = os.path.join(self.output_dir, file_name)

        with gzip.open(file_path, 'wb') as f:
            f.write(formatted_data)
