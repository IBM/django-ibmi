# Contributing 

## Contributing in General

Our project welcomes external contributions. If you see something that you want to change, please feel free to. 

To contribute code or documentation, please submit a [pull request](https://github.com/IBM/django-ibmi/pulls).

A good way to familiarize yourself with the codebase and contribution process is
to look for and tackle low-hanging fruit in the [issue tracker](https://github.com/IBM/django-ibmi/issues).
These will be marked with the [good first issue](https://github.com/IBM/django-ibmi/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) label. 
You may also want to look at those marked with [help wanted](https://github.com/IBM/django-ibmi/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22).

**Note: We appreciate your effort, and want to avoid a situation where a contribution
requires extensive rework (by you or by us), sits in backlog for a long time, or
cannot be accepted at all!**

### Proposing a new feature

When you would like to propose a new feature, please create an [issue](https://github.com/IBM/django-ibmi/issues), 
so that the feature may be discussed before creating a pull request. This allows us to decide whether the feature will be
accepted into the code base before you put in valuable and precious time coding that feature.

### Fixing bugs

If you are looking to fix a bug, again, please create an [issue](https://github.com/IBM/django-ibmi/issues) prior to opening a pull request so it can
 be tracked. 


## Legal

We have tried to make it as easy as possible to make contributions. This
applies to how we handle the legal aspects of contribution. We use the
same approach - the [Developer's Certificate of Origin 1.1 (DCO)](https://github.com/hyperledger/fabric/blob/master/docs/source/DCO1.1.txt) - that the Linux® Kernel [community](https://elinux.org/Developer_Certificate_Of_Origin)
uses to manage code contributions.

We simply ask that when submitting a patch for review, the developer
must include a sign-off statement in the commit message.

Here is an example Signed-off-by line, which indicates that the
submitter accepts the DCO:

```text
Signed-off-by: John Doe <john.doe@example.com>
```

You can include this automatically when you commit a change to your
local git repository using the following command:

```bash
git commit -s
```

# Install
```
pip install .
```

# Testing 
```
 * Create a new Django project by executing "django-admin.py startproject myproj".
 * Now go to this newly create directory, and edit settings.py file to access DB2.
 * In case of nix the steps will be like:
  {{{
  $ django-admin.py startproject myproj
  $ cd myproj
  $ vi settings.py
  }}}
 * The settings.py will be like (after adding DB2 properties):
   {{{
   DATABASES = {
      'default': {
         'ENGINE'     : 'ibm_db_django',
         'NAME'       : 'mydb',
         'USER'       : 'db2inst1',
         'PASSWORD'   : 'ibmdb2',
         'HOST'       : 'localhost',
         'PORT'       : '50000',
         'PCONNECT'   :  True,      #Optional property, default is false
      }
   }
   }}}
   
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
   $ Python manage.py test django.contrib.auth #For Django-1.6.x onwards, since test discovery behavior have changed
   }}} 
 * For Windows, steps are same as above. In case of editing settings.py file, use notepad (or any other) editor.
```

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.