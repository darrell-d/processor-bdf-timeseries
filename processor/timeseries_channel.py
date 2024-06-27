import uuid

class TimeSeriesChannel():
    def __init__(self, name, id = None):
        self.id = id if id else uuid.uuid4()
        self.name = name

    def __repr__(self):
        return f"TimeSeriesChannel(id = {self.id}, name = {self.name})"
