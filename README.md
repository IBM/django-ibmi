# Django Adapter for IBM i 

[IBM i](https://en.wikipedia.org/wiki/IBM_i) support for the [Django](https://www.djangoproject.com/) application framework.

django-ibmi enables access to Db2 for IBM i from Django applications.

The adapter has been developed and is supported by IBM.

**Please note that this project is still in active development and is not ready for use.** :rotating_light: 

```
 * Create a new Django project by executing "django-admin.py startproject myproj".
 * Now go to this newly create directory, and edit settings.py file to
 access Db2 for i.
 * The steps go as follows:
  1. In shell or cmd line run:
        django-admin.py startproject myproj
  2. Edit settings.py file
     * The settings.py will be like (after adding Db2 properties):
       {{{
       DATABASES = {
          'default': {
             'ENGINE'     : 'django-ibmi',
             'NAME'       : 'ibmi-sysname',
             'USER'       : 'uid',
             'PASSWORD'   : 'pwd',
          }
       }
       }}}
```

# Prerequisites for Django on Python 

 * Django 2.0 or higher
 * pyodbc 4.0 or higher
 
# Installation 

## 1. Install Django 

Follow [these](http://docs.djangoproject.com/en/dev/topics/install/#installing-an-official-release-website) instructions to install django.

## 2. Install IBM i Django adapter (django-ibmi)

TODO
 
# Documentation

TODO

# Tested Operating Systems 

TODO

# Testing 
```
 * Follow steps above to setup up Django with the django-ibmi adapter
   
 * Change USE_TZ to False
 
 * RUN python manage.py migrate
 
 * In the tuple INSTALLED_APPS in settings.py add the following lines:
   {{{
   'django.contrib.flatpages',
   'django.contrib.redirects',
   'django.contrib.comments',
   'django.contrib.admin',
   }}}
 * Next step is to run a simple test suite. To do this just execute following command in the project we created earlier:
   {{{
   $ python manage.py test #for Django-1.5.x or older
   $ Python manage.py test django.contrib.auth 
   }}} 
```
# Database Transactions 

 *  Django by default executes without transactions i.e. in auto-commit mode. This default is generally not what you want in web-applications. [http://docs.djangoproject.com/en/dev/topics/db/transactions/ Remember to turn on transaction support in Django]

# Known Limitations of django-ibmi adapter 

 * Non-standard SQL queries are not supported. e.g. "SELECT ? FROM TAB1"
 * For updations involving primary/foreign key references, the entries should be made in correct order. Integrity check is always on and thus the primary keys referenced by the foreign keys in the referencing tables should always exist in the parent table.
 * Db2 Timestamps do not support timezone aware information. Thus a
  Datetime field including tzinfo(timezone aware info) would fail.

# Contributing to the django-ibmi python project

 Please read the [contribution guidelines](https://github.com/IBM/django-ibmi/blob/master/contributing/CONTRIBUTING.md)

  The developer sign-off should include the reference to the DCO in remarks(example below):
  DCO 1.1 Signed-off-by: Random J Developer <random@developer.org>

