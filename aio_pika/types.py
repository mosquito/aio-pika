from typing import Union, Callable, Any
import aiormq.types


ReturnCallbackType = Callable[[aiormq.types.DeliveredMessage], Any]
CloseCallbackType = Callable[[Exception], Any]
TimeoutType = Union[int, float, None]
ExchangeType = Union['Exchange', str]
