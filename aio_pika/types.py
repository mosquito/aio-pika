from typing import Union, Callable, Any, Optional
import aiormq.types


ReturnCallbackType = Callable[[aiormq.types.DeliveredMessage], Any]
CloseCallbackType = Callable[[Optional[BaseException]], None]
TimeoutType = Union[int, float]
ExchangeType = Union['Exchange', str]
