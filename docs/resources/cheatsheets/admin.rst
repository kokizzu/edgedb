.. _ref_cheatsheet_admin:

Administering an instance
=========================

Create a schema branch:

.. code-block:: edgeql-repl

    db> create schema branch my_new_feature from main;
    OK: CREATE BRANCH


Create a data branch:

.. code-block:: edgeql-repl

    db> create data branch my_new_feature from main;
    OK: CREATE BRANCH


Create an empty branch:

.. code-block:: edgeql-repl

    db> create empty branch my_new_feature;
    OK: CREATE BRANCH


.. note::
    Prior to |Gel| and |EdgeDB| 5.0 *branches* were called *databases*.
    A command to create a new empty *database* is ``create database``
    (still supported for backwards compatibility).

    .. code-block:: edgeql-repl

        db> create database my_new_feature;
        OK: CREATE DATABASE


Create a role:

.. code-block:: edgeql-repl

    db> create superuser role project;
    OK: CREATE ROLE



Configure passwordless access (such as to a local development database):

.. code-block:: edgeql-repl

    db> configure instance insert Auth {
    ...     # Human-oriented comment helps figuring out
    ...     # what authentication methods have been setup
    ...     # and makes it easier to identify them.
    ...     comment := 'passwordless access',
    ...     priority := 1,
    ...     method := (insert Trust),
    ... };
    OK: CONFIGURE INSTANCE



Set a password for a role:

.. code-block:: edgeql-repl

    db> alter role project
    ...     set password := 'super-password';
    OK: ALTER ROLE



Configure access that checks password (with a higher priority):

.. code-block:: edgeql-repl

    db> configure instance insert Auth {
    ...     comment := 'password is required',
    ...     priority := 0,
    ...     method := (insert SCRAM),
    ... };
    OK: CONFIGURE INSTANCE



Remove a specific authentication method:

.. code-block:: edgeql-repl

    db> configure instance reset Auth
    ... filter .comment = 'password is required';
    OK: CONFIGURE INSTANCE



Run a script from command line:

.. cli:synopsis::

    cat myscript.edgeql | gel [<connection-option>...]
