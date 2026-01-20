from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return (f"Result: {result}")


class NumericProcessor(DataProcessor):
    def process(self, data: List[int]) -> str:
        try:
            length = len(data)
            addition = sum(data)
            return (f"Processed {length} numeric values, sum={addition}, \
avg={addition / length}")
        except TypeError:
            return (f"ERROR: {data} is not the right for this processor \
(need List[int])")
        except ZeroDivisionError:
            return ("ERROR: data est vide")

    def validate(self, data: List[int]) -> bool:
        try:
            length = len(data)
            sum(data)
            if (length > 0):
                return (True)
            return (False)
        except TypeError:
            return (False)

    def format_output(self, result: str) -> str:
        return (f"Output: {result}")


class TextProcessor(DataProcessor):
    def process(self, data: str) -> str:
        if (type(data) is str):
            return (f"Processed text: {len(data)} characters, \
{len(data.split())} words")
        else:
            return (f"ERROR: {data} is not the right for this processor \
(need str)")

    def validate(self, data: str) -> bool:
        if (type(data) is str):
            return (True)
        return (False)

    def format_output(self, result: str) -> str:
        return (f"Output: {result}")


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        alert = data.split(":")
        if (alert[0] == "ERROR"):
            level = "ALERT"
        elif (alert[0] == "INFO"):
            level = "INFO"
        else:
            level = "WARN"
        return (f"[{level}] {alert[0]} level detected:{alert[1]}")

    def validate(self, data: str) -> bool:
        if (type(data) is not str):
            return (False)
        alert = data.split(":")
        if (len(alert) != 2):
            return (False)
        return (True)

    def format_output(self, result: str) -> str:
        return (f"Output: {result}")


if (__name__ == "__main__"):
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    print("Initializing Numeric Processor...")

    data = [1, 2, 3, 4, 5]
    processor = NumericProcessor()
    print("Processing data:", data)
    process = processor.process(data)
    print("Validation: ", end="")
    if (processor.validate(data)):
        print("Numeric data verified")
    else:
        print("Not a numeric data")
    print(processor.format_output(process))

    print("\nInitializing Text Processor...")
    data = "Hello Nexus World"
    processor = TextProcessor()
    print(f"Processing data: \"{data}\"")
    process = processor.process(data)
    print("Validation: ", end="")
    if (processor.validate(data)):
        print("Text data verified")
    else:
        print("Not a text data")
    print(processor.format_output(process))

    print("\nInitializing Log Processor...")
    data = "ERROR: Connection timeout"
    processor = LogProcessor()
    print(f"Processing data: \"{data}\"")
    process = processor.process(data)
    print("Validation: ", end="")
    if (processor.validate(data)):
        print("Log entry verified")
    else:
        print("Not a log entry")
    print(processor.format_output(process))

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    print("Result 1:", NumericProcessor().process([2, 2, 2]))
    print("Result 2:", TextProcessor().process("Hello World"))
    print("Result 3:", LogProcessor().process("INFO:  System ready"))
    print("\nFoundation systems online. Nexus ready for advanced streams.")
