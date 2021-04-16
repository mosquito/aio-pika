from typing import Any, Callable, Optional, Union

import aiormq.abc


Sender = Any
ReturnCallbackType = Callable[[Sender, aiormq.abc.DeliveredMessage], Any]
CloseCallbackType = Callable[[Sender, Optional[BaseException]], None]
TimeoutType = Union[int, float]
