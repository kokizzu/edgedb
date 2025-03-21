.. _ref_guide_using_projects:

================
Create a project
================

Projects are the most convenient way to develop applications with Gel. This
is the recommended approach.

To get started, navigate to the root directory of your codebase in a shell and
run :gelcmd:`project init`. You'll see something like this:

.. code-block:: bash

  $ gel project init
  No `gel.toml` found in this repo or above.
  Do you want to initialize a new project? [Y/n]
  > Y
  Checking Gel versions...
  Specify the version of Gel to use with this project [1-rc3]:
  > # left blank for default
  Specify the name of Gel instance to use with this project:
  > my_instance
  Initializing Gel instance...
  Bootstrap complete. Server is up and running now.
  Project initialialized.

Let's unpack that.

1. First, it asks you to specify a Gel version, defaulting to the most
   recent version you have installed. You can also specify a version you
   *don't* have installed, in which case it will be installed.
2. Then it asks you how you'd like to run Gel: locally, in a Docker image,
   or in the cloud (coming soon!).
3. Then it asks for an instance name. If no instance currently exists with this
   name, it will be created (using the method you specified in #2).
4. Then it **links** the current directory to that instance. A "link" is
   represented as some metadata stored in Gel's :ref:`config directory
   <ref_cli_gel_paths>`—feel free to peek inside to see how it's stored.
5. Then it creates an :ref:`ref_reference_gel_toml` file, which marks this
   directory as a Gel project.
6. Finally, it creates a ``dbschema`` directory and a :dotgel:`dbschema/default`
   schema file (if they don't already exist).


FAQ
---

How does this help me?
^^^^^^^^^^^^^^^^^^^^^^

Once you've initialized a project, your project directory is *linked* to a
particular instance. That means, you can run CLI commands without connection
flags. For instance, :gelcmd:`-I my_instance migrate` becomes simply
:gelcmd:`migrate`. The CLI detects the existence of the |gel.toml| file, reads
the current directory, and checks if it's associated with an existing project.
If it is, it looks up the credentials of the linked instance (they're stored in
a :ref:`standardized location <ref_cli_gel_paths>`), uses that information to
connect to the instance, and applies the command.

Similarly, all :ref:`client libraries <ref_clients_index>` will use the same
mechanism to auto-connect inside project directories, no hard-coded credentials
required.

.. code-block:: typescript-diff

      import gel from "gel";

    - const pool = gel.createPool("my_instance");
    + const pool = gel.createPool();

What do you mean *link*?
^^^^^^^^^^^^^^^^^^^^^^^^

The "link" is just metaphor that makes projects easier to think about; in
practice, it's just a bit of metadata we store in the Gel :ref:`config
directory <ref_cli_gel_paths>`. When the CLI or client libraries try to
connect to an instance, they read the currect directory and cross-reference it
against the list of initialized projects. If there's a match, it reads the
credentials of the project's associated instance and auto-connects.

How does this work in production?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It doesn't. Projects are intended as a convenient development tool that make it
easier to develop Gel-backed applications locally. In production, you should
provide instance credentials to your client library of choice using environment
variables. See :ref:`Connection parameters <ref_reference_connection>` page for
more information.


What's the |gel.toml| file?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The most important role of |gel.toml| is to mark a directory as an
instance-linked project, but it can also specify the server version and the
schema directory for a project. The server version value in the generated
|gel.toml| is determined by the Gel version you selected when you ran
:ref:`ref_cli_gel_project_init`.

Read :ref:`our reference documentation on gel.toml <ref_reference_gel_toml>`
to learn more.

.. note::

    If you're not familiar with the TOML file format, it's a very cool, minimal
    language for config files designed to be simpler than JSON or YAML. Check
    out `the TOML documentation <https://toml.io/en/v1.0.0>`_.


How do I use :gelcmd:`project` for existing codebases?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you already have an project on your computer that uses Gel, follow these
steps to convert it into a Gel project:

1. Navigate into the project directory (the one containing you ``dbschema``
   directory).
2. Run :gelcmd:`project init`.
3. When asked for an instance name, enter the name of the existing local
   instance you use for development.

This will create |gel.toml| and link your project directory to the
instance. And you're done! Try running some commands without connection flags.
Feels good, right?

How does this make projects more portable?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's say you just cloned a full-stack application that uses Gel. The
project directory already contains an |gel.toml| file. What do you do?

Just run :gelcmd:`project init` inside the directory! This is the beauty of
:gelcmd:`project`. You don't need to worry about creating an instance with a
particular name, running on a particular port, creating users and passwords,
specifying environment variables, or any of the other things that make setting
up local databases hard. Running :gelcmd:`project init` will install the
necessary version of Gel (if you don't already have it installed), create an
instance, apply all unapplied migrations. Then you can start up the application
and it should work out of the box.


How do I unlink a project?
^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to remove the link between your project and its linked instance,
run :gelcmd:`project unlink` anywhere inside the project. This doesn't affect
the instance, it continues running as before. After unlinking, can run
:gelcmd:`project init` inside project again to create or select a new instance.


.. code-block:: bash

  $ gel project init
  No `gel.toml` found in `~/path/to/my_project` or above.
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the name of Gel instance to use with this project
  [default: my_project]:
  > my_project
  Checking Gel versions...
  Specify the version of Gel to use with this project [default: x.x]:
  > x.x


How do I use :gelcmd:`project` with a non-local instance?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes you may want to work on a Gel instance that is just not in your
local development environment, like you may have a second workstation, or you
want to test against a staging database shared by the team.

This is totally a valid case and Gel fully supports it!

Before running :gelcmd:`project init`, you just need to create a local link to
the remote Gel instance first:

.. TODO: Will need to change this once https://github.com/geldata/gel-cli/issues/1269 is resolved

.. lint-off

.. code-block:: bash

  $ gel instance link
  Specify the host of the server [default: localhost]:
  > 192.168.4.2
  Specify the port of the server [default: 5656]:
  > 10818
  Specify the database user [default: admin]:
  > admin
  Specify the branch name [default: main]:
  > main
  Unknown server certificate: SHA1:c38a7a90429b033dfaf7a81e08112a9d58d97286. Trust? [y/N]
  > y
  Password for 'admin':
  Specify a new instance name for the remote server [default: 192_168_4_2_10818]:
  > staging_db
  Successfully linked to remote instance. To connect run:
    gel -I staging_db

.. lint-on

Then you could run the normal :gelcmd:`project init` and use ``staging_db`` as
the instance name.

.. note::

  When using an existing instance, make sure that the project source tree is in
  sync with the current migration revision of the instance. If the current
  revision in the database doesn't exist under ``dbschema/migrations/``, it'll
  raise an error trying to migrate or create new migrations. In this case, you
  should update your local source tree to the revision that matches the current
  revision of the database.
