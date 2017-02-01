#!/usr/bin/python

import os
import sys

# START UGLY COPY FROM mysite.fcgi
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.dirname(os.path.join(THIS_DIR, '..'))

# Add a custom Python path.
sys.path.append(ROOT_DIR)
# END UGLY COPY FROM mysite.fcgi

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings_heroku")

# This application object is used by the development server
# as well as any WSGI server configured to use this file.
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
