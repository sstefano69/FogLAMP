"""
Authentication-related utilities
"""

from datetime import datetime
from aiohttp import web
import jwt
import json
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from foglamp.admin_api.model import User

# It needs to be changed to a random value which is kept secret and is unique to your deployment.
# You should generate a key using a cryptographically-secure pseudo-random number generator such
# as the `os.urandom` Python function on modern systems. In short: `os.urandom(32)`.
# This will be moved to something that interacts with the configuration service
JWT_SECRET = 'secret'
JWT_ALGORITHM = 'HS256'
JWT_EXP_DAYS = 7
JWT_REFRESH_MINUTES = 15

foglamp_auth_type = os.environ.get('FOGLAMP_AUTH_TYPE')
if foglamp_auth_type is None or foglamp_auth_type not in ['HMAC', 'RSA']:
    foglamp_auth_type = 'HMAC'
    JWT_ALGORITHM = 'HS256'

if foglamp_auth_type == 'RSA':
    JWT_ALGORITHM = 'RS256'

privateRsaKey = '''
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCyTRbxdRzOKy7k
NdX6HEA681rK5Z4Un8usL4gqybxKdapG58Cnp640t/dFPWz9JnXlWxHuGhNP3nb8
3dfI3O7oLhE/XrM0d10YhwMvC9S90lT05gxuqMny9k5Lv9S5/gmNiT0Vi1xRDV1n
stmTyCuL0J2plncXC5TDjrUcabLaZ+NAGtrh6xL6mZwmDyP2s8l32xztUS9f86Lx
BOJ4yESUGwgCqC5TSYIiDMDo2ztyXNCQ1yRaWD9b9vPMICtIsJpmjZU+yqbDi2Pm
cq6HAXa044mjltKJxu138OPzKqgJ82iz6bQ36FzaEocUbRMDa9L3PPhqdcq3m2jA
uufMfTgBAgMBAAECggEABoeML4t6YpJyRSkzjNU8BOykhomiIF85tjSHaAm7O37T
nYUfKQSa/JNAGukE9MBT0PJk1bNewa4D7X6ySEjO9vG47/kn1uQIJL3NAAwO3mS1
d8J08hh8TMl4VI3R3H3RG73K3t8bolw673E9RowuDBC3tV4tPkrOR9sSBthYFGdp
3h9EifvIdV8dnGQKMczR/EkiS0JyecrX/yU0KcEhtBQB6asyoACPZBN2vanztGYq
wLhG9pQHuvBq/wz3JmiEHlw2+NrCxLI5AQsKcjvMYXMDSwQ9wqRDYHdEXUsCrxlH
7I8tRAkfGd4l64QzcX+QmX2EJRQcmKTXhzyTY/4oAQKBgQDitRIy55U68qtapf59
wdAiNYCKgsvC1jZMETRN73wzCnoWnIrI/pUcn6bq9umKi5Bb9yOILAs/do24Jm/u
MztJIj2KirJjH4iCcCj+GsZMcpCId8GisLit9+c9QkKoAibjWoparb/dy01+5BKu
azLt+LdGJvKQjWIpO1nh1WgsGQKBgQDJVtv/vNxo+Xgtf+OSz3KcDELUgeim90CP
1d+WWqcBnzTIr89I1F1r1203GtSIHhDQBprlsIqeueARCYhS6R7ceuPz+eIeJuRn
8LyEVxXEJxUPCOVRPz1kitlZZ5YVI4k2ODA4xTXQqKm/el3q0DirtVYpdCAP2CZ0
2JFiEIRoKQKBgQCtOWX6d3FTZXEBBqz6Mklymibprx86dKtwXAT3N/JSncLD3EE/
aydMg8Dq8FdqgVGAs8n2lpDRoSiQeO41t/eo3fzgCzJbAxEvebwYzSSP7Un6vGa8
hQPYCgAqkqKT9XD5b4kf2HsuYdWJC4QqucZkhxKjC8d0JFPI5Yy8PnAmoQKBgBDW
o9WSjPh2jLd5b6XylYo5eElfh/6WxR6Ca42ejUMUdoymL3bO0/VV8fte0hGEOjG7
q660AhPeRcAHBRCyEHOuD2xzJJcS9509w/ZpdheOkTNNsigGWvMSrbTNdf7UlwAi
2N2WmLaXtIKgEES4H1U0+DzFSn0ovaaeCiKicQBpAoGAZ5bC9cdrGkTVOGjWDLmO
bawXDpxifJhh+mKeQR3Kxht/lV3lQqesKMw0lkzKcOn+MDhTOoIEA0XkOJ48uJNt
ntbBHV4aEu7Uky9Rhgnqg7mc8CrNWZiPv+SdygZ2LyhFoZbA2VBY1++QZXBFU3TK
0SHpr4xDOPDMNBfuIG9Sap4=
-----END PRIVATE KEY-----
'''

publicRsaKey = '''
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsk0W8XUczisu5DXV+hxA
OvNayuWeFJ/LrC+IKsm8SnWqRufAp6euNLf3RT1s/SZ15VsR7hoTT952/N3XyNzu
6C4RP16zNHddGIcDLwvUvdJU9OYMbqjJ8vZOS7/Uuf4JjYk9FYtcUQ1dZ7LZk8gr
i9CdqZZ3FwuUw461HGmy2mfjQBra4esS+pmcJg8j9rPJd9sc7VEvX/Oi8QTieMhE
lBsIAqguU0mCIgzA6Ns7clzQkNckWlg/W/bzzCArSLCaZo2VPsqmw4tj5nKuhwF2
tOOJo5bSicbtd/Dj8yqoCfNos+m0N+hc2hKHFG0TA2vS9zz4anXKt5towLrnzH04
AQIDAQAB
-----END PUBLIC KEY-----
'''

def json_response(body='', **kwargs):
    kwargs['body'] = json.dumps(body or kwargs['body']).encode('utf-8')
    kwargs['content_type'] = 'text/json'
    return web.Response(**kwargs)

def authentication_required(func):
    """Defines a decorator @authentication_required that should be added to all
    URI handlers that require authentication."""
    def wrapper(request):
        """Verify user is logged in and short-duration token has not expired"""
        if not request.user:
            return web.json_response({'message': 'Authentication required'}, status=403)

        try:
            if not request.jwt_payload['access']:
                return web.json_response({'message': 'Not an access token'},
                                         status=400)
        except Exception:
            return web.json_response({'message': 'Not an access token'},
                                     status=400)

        return func(request)
    return wrapper


async def auth_middleware(app, handler):
    #pylint: disable=unused-argument
    """This method is called for every REST request. It inspects
    the token (if there is one) for validity and checks whether
    it has expired.
    """

    async def middleware(request):
        """If there is an authorization header, this function confirms the
        validity of it and checks whether the token's long duration
        has expired.
        """
        request.user = None
        jwt_token = request.headers.get('authorization', None)
        if jwt_token:
            try:
                if foglamp_auth_type == 'HMAC':
                    request.jwt_payload = jwt.decode(jwt_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
                else:
                    request.jwt_payload = jwt.decode(jwt_token, publicRsaKey, algorithms=[JWT_ALGORITHM])
            except jwt.DecodeError:
                return web.json_response({'message': 'Token is invalid'},
                                         status=400)
            except jwt.ExpiredSignatureError:
                return web.json_response({'message': 'Token expired'},
                                         status=401)
            request.user = User.objects.get(id=request.jwt_payload['user_id'])
        return await handler(request)
    return middleware

