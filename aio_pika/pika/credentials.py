"""The credentials classes are used to encapsulate all authentication
information for the :class:`~pika.connection.ConnectionParameters` class.

The :class:`~pika.credentials.PlainCredentials` class returns the properly
formatted username and password to the :class:`~pika.connection.Connection`.

To authenticate with Pika, create a :class:`~pika.credentials.PlainCredentials`
object passing in the username and password and pass it as the credentials
argument value to the :class:`~pika.connection.ConnectionParameters` object.

If you are using :class:`~pika.connection.URLParameters` you do not need a
credentials object, one will automatically be created for you.

If you are looking to implement SSL certificate style authentication, you would
extend the :class:`~pika.credentials.ExternalCredentials` class implementing
the required behavior.

"""
import abc
import logging
from ..amqp.codec import as_bytes


log = logging.getLogger(__name__)


class BaseCredentials:
    TYPE = None

    @abc.abstractmethod
    def _make_response(self):
        raise NotImplementedError

    def response_for(self, start):
        """Validate that this type of authentication is supported

        :param spec.Connection.Start start: Connection.Start method
        :rtype: tuple(str|None, str|None)

        """
        if self.TYPE not in as_bytes(start.mechanisms).split():
            return None, None

        return self._make_response()

    def erase_credentials(self):
        """Called by Connection when it no longer needs the credentials"""
        log.debug('Not supported by this Credentials type')


class PlainCredentials(BaseCredentials):
    """A credentials object for the default authentication methodology with
    RabbitMQ.

    If you do not pass in credentials to the ConnectionParameters object, it
    will create credentials for 'guest' with the password of 'guest'.

    If you pass True to erase_on_connect the credentials will not be stored
    in memory after the Connection attempt has been made.

    :param str username: The username to authenticate with
    :param str password: The password to authenticate with
    :param bool erase_on_connect: erase credentials on connect.

    """

    __slots__ = '__username', '__password', 'erase_on_connect'

    TYPE = as_bytes('PLAIN')

    def __init__(self, username, password, erase_on_connect=False):
        """Create a new instance of PlainCredentials

        :param str username: The username to authenticate with
        :param str password: The password to authenticate with
        :param bool erase_on_connect: erase credentials on connect.

        """
        self.__username = as_bytes(username)
        self.__password = as_bytes(password)
        self.erase_on_connect = erase_on_connect

    def _make_response(self):
        return (
            PlainCredentials.TYPE,
            b'\0' + self.__username + b'\0' + self.__password
        )

    def erase_credentials(self):
        """Called by Connection when it no longer needs the credentials"""
        if self.erase_on_connect:
            log.info("Erasing stored credential values")
            self.__username = None
            self.__password = None


class ExternalCredentials(BaseCredentials):
    """The ExternalCredentials class allows the connection to use EXTERNAL
    authentication, generally with a client SSL certificate.

    """

    TYPE = as_bytes('EXTERNAL')

    def _make_response(self):
        return ExternalCredentials.TYPE, b''

    def __init__(self):
        """Create a new instance of ExternalCredentials"""
        self.erase_on_connect = False


# Append custom credential types to this list for validation support
VALID_TYPES = [PlainCredentials, ExternalCredentials]
