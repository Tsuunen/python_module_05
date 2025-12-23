from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.__id: str = stream_id
        self.data: List[Any] = []

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def get_id(self) -> str:
        return (self.__id)

    def get_data(self) -> List[Any]:
        return (self.data)

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return ([data for data in data_batch if data != criteria])

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return ({
            "count": len(self.data)
        })


class SensorStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.type = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        self.data = data_batch
        return (f"{len(data_batch)} readings processed")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        temp_avg: List[int] = [0, 0]
        hum_avg: List[int] = [0, 0]
        pres_avg: List[int] = [0, 0]
        for data in self.data:
            if (data[0] == "temp"):
                temp_avg[0] += data[1]
                temp_avg[1] += 1
            elif (data[0] == "humidity"):
                hum_avg[0] += data[1]
                hum_avg[1] += 1
            elif (data[0] == "pressure"):
                pres_avg[0] += data[1]
                pres_avg[1] += 1
        temp = temp_avg[0] / temp_avg[1]
        hum = hum_avg[0] / hum_avg[1]
        pres = pres_avg[0] / pres_avg[1]
        return ({
            "count": len(self.data),
            "temp_avg": temp,
            "hum_avg": hum,
            "pres_avg": pres
        })

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return ([data for data in data_batch if data[1] >= criteria])


class TransactionStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        self.data = data_batch
        return (f"{len(data_batch)} operations processed")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        profit = 0
        for data in self.data:
            if (data[0] == "buy"):
                profit += data[1]
            elif (data[0] == "sell"):
                profit -= data[1]
        return ({
            "count": len(self.data),
            "profit": profit
        })

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return ([data for data in data_batch if data[1] >= criteria])


class EventStream(DataStream):
    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        self.data = data_batch
        return (f"{len(data_batch)} events processed")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        count = {}
        for e in self.data:
            if (count.get(e)):
                count[e] += 1
            else:
                count[e] = 1
        count["count"] = len(self.data)
        return (count)

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return ([data for data in data_batch if data == criteria])


class StreamProcessor:
    def __init__(self) -> None:
        self.__streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.__streams.append(stream)

    def filter(self) -> List[Any]:
        ret = []
        for stream in self.__streams:
            if (isinstance(stream, SensorStream)):
                criteria = 20
            elif (isinstance(stream, TransactionStream)):
                criteria = 500
            elif (isinstance(stream, EventStream)):
                criteria = "error"
            ret += stream.filter_data(stream.get_data(), criteria)
        return (ret)


if (__name__ == "__main__"):
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    stream = SensorStream("SENSOR_001")
    data: List[Any] = [
        ("temp", 22.5),
        ("humidity", 65),
        ("pressure", 1013),
    ]
    print(f"Stream ID: {stream.get_id()}, Type: {stream.type}")
    print("Processing sensor batch:", data)
    print(
        f"Sensor analysis: {stream.process_batch(data)}, \
avg temp: {stream.get_stats()['temp_avg']}°C")

    print("\nInitializing Transaction Stream...")
    stream = TransactionStream("TRANS_001")
    data = [
        ("buy", 100),
        ("sell", 150),
        ("buy", 75),
    ]
    print(f"Stream ID: {stream.get_id()}, Type: {stream.type}")
    print("Processing sensor batch:", data)
    print(f"Transaction analysis: {stream.process_batch(data)}, \
net flow: {stream.get_stats()['profit']:+} units")

    print("\nInitializing Event Stream...")
    stream = EventStream("EVENT_001")
    data = ["login", "error", "logout"]
    print(f"Stream ID: {stream.get_id()}, Type: {stream.type}")
    print("Processing sensor batch:", data)
    print(f"Event analysis: {stream.process_batch(data)}, \
{stream.get_stats()['error']} error detected")

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    proc = StreamProcessor()
    print("Batch 1 Results:")
    stream = SensorStream("SENSOR_002")
    print("- Sensor data:", stream.process_batch([("temp", 22.5),
                                                  ("humidity", 120)]))
    proc.add_stream(stream)
    stream = TransactionStream("TRANS_002")
    print("- Sensor data:", stream.process_batch([("buy", 22.5),
                                                  ("sell", 65),
                                                  ("sell", 30),
                                                  ("buy", 600)]))
    proc.add_stream(stream)
    stream = EventStream("EVENT_002")
    print("- Event data:", stream.process_batch(["error", "logout", "warn"]))
    proc.add_stream(stream)
    print("\nStream filtering active: High-priority data only")
    filter = proc.filter()
    sensor = [f for f in filter if len(
        f) == 2 and f[0] != "buy" and f[0] != "sell"]
    transaction = [f for f in filter if len(
        f) == 2 and (f[0] == "buy" or f[0] == "sell")]
    print(f"Filtered results: {len(sensor)} critical sensor alerts, \
{len(transaction)} large transaction")
    print("\nAll streams processed successfully. Nexus throughput optimal.")
