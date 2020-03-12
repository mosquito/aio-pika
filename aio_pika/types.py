from typing import Union, Callable, Any, Optional
import aiormq.types


Sender = Any
ReturnCallbackType = Callable[[Sender, aiormq.types.DeliveredMessage], Any]
CloseCallbackType = Callable[[Sender, Optional[BaseException]], None]
TimeoutType = Union[int, float]
ExchangeType = Union['Exchange', str]
