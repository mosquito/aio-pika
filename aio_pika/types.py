from typing import Any, Callable, Optional, Union

import aiormq.types


Sender = Any
ReturnCallbackType = Callable[[Sender, aiormq.types.DeliveredMessage], Any]
CloseCallbackType = Callable[[Sender, Optional[BaseException]], None]
TimeoutType = Union[int, float]
