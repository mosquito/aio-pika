from typing import Union, Callable, Any
import aiormq.types


ReturnCallbackType = Callable[[aiormq.types.DeliveredMessage], Any]
CloseCallbackType = Callable[[Exception], Any]
TimeoutType = Union[int, float]
ExchangeType = Union['Exchange', str]
