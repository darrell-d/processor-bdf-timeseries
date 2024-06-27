import os
import logging

from pynwb import NWBHDF5IO
from pynwb.ecephys import ElectricalSeries

from writer import TimeSeriesChunkWriter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

log = logging.getLogger()

if __name__ == "__main__":
    input_dir = os.environ.get('INPUT_DIR')
    output_dir = os.environ.get('OUTPUT_DIR')
    chunk_size_mb = os.environ.get('CHUNK_SIZE_MB', 1)

    bytes_per_mb = pow(2, 20)
    bytes_per_sample = 8 # 64-bit floating point value
    chunk_size = int(chunk_size_mb * BYTES_PER_MB / BYTES_PER_SAMPLE)

    input_files = [
        f.path
        for f in os.scandir(input_dir)
        if f.is_file() and os.path.splitext(f.name)[1].lower() == '.nwb'
    ]

    assert len(input_files) == 1, "NWB post processor only supports a single file as input"

    with NWBHDF5IO(input_files[0], mode="r") as io:
        nwb = io.read()
        electrical_series = [acq for acq in nwb.acquisition.values() if type(acq) == ElectricalSeries]
        if len(electrical_series) < 1:
            log.error('NWB file has no continuous raw electrical series data')
        if len(electrical_series) > 1:
            log.warn('NWB file has multiple raw electrical series acquisitions')

        writer = TimeSeriesChunkWriter(nwb.session_start_time, output_dir, chunk_size)

        for series in electrical_series:
            writer.write_electrical_series(series)
