author_info = (
    ('Dmitry Orlov', 'me@mosquito.su'),
)

package_info = "Wrapper for the PIKA for asyncio and humans."
package_license = "Apache Software License"

team_email = 'me@mosquito.su'

version_info = (2, 3, 0)

__author__ = ", ".join("{} <{}>".format(*info) for info in author_info)
__version__ = ".".join(map(str, version_info))
