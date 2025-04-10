import numpy as np
from timeseries_channel import TimeSeriesChannel
import logging

log = logging.getLogger()

class BDFElectricalSeriesReader:
    def __init__(self, edf_reader, session_start_time):
        self.reader = edf_reader
        self.session_start_time_secs = session_start_time.timestamp()
        self.num_channels = self.reader.signals_in_file
        self.num_samples = self.reader.getNSamples()[0]
        self.sampling_rate = self.reader.getSampleFrequency(0)

        self._timestamps = np.linspace(
            0, self.num_samples / self.sampling_rate, self.num_samples, endpoint=False
        ) + self.session_start_time_secs

        self._channels = None

        # Gather scaling info per channel
        self.scale_info = []
        for ch in range(self.num_channels):
            dmin = self.reader.getDigitalMinimum(ch)
            dmax = self.reader.getDigitalMaximum(ch)
            pmin = self.reader.getPhysicalMinimum(ch)
            pmax = self.reader.getPhysicalMaximum(ch)
            self.scale_info.append((dmin, dmax, pmin, pmax))

    @property
    def timestamps(self):
        return self._timestamps

    @property
    def channels(self):
        if self._channels is None:
            self._channels = []
            for ch in range(self.num_channels):
                label = self.reader.getLabel(ch)
                self._channels.append(
                    TimeSeriesChannel(
                        index=ch,
                        name=label,
                        rate=self.sampling_rate,
                        start=self.timestamps[0] * 1e6,
                        end=self.timestamps[-1] * 1e6,
                        group=""
                    )
                )
        return self._channels

    def contiguous_chunks(self):
        gap_threshold = (1.0 / self.sampling_rate) * 2
        boundaries = np.concatenate(
            ([0], (np.diff(self.timestamps) > gap_threshold).nonzero()[0] + 1, [len(self.timestamps)])
        )
        for i in range(len(boundaries) - 1):
            yield boundaries[i], boundaries[i + 1]

    def get_chunk(self, channel_index, start=None, end=None):
        """
        Reads raw digital samples and scales to physical values using per-channel conversion
        """
        if start is None:
            start = 0
        if end is None:
            end = self.num_samples

        raw = self.reader.readSignal(channel_index)[start:end]

        dmin, dmax, pmin, pmax = self.scale_info[channel_index]
        scale = (pmax - pmin) / (dmax - dmin)
        physical = (raw - dmin) * scale + pmin

        return physical
