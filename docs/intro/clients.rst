.. _ref_intro_clients:

================
Client Libraries
================

|Gel| implements libraries for popular languages that make it easier to work
with Gel. These libraries provide a common set of functionality.

- *Instantiating clients.* Most libraries implement a ``Client`` class that
  internally manages a pool of physical connections to your Gel instance.
- *Resolving connections.* All client libraries implement a standard protocol
  for determining how to connect to your database. In most cases, this will
  involve checking for special environment variables like :gelenv:`DSN` or, in
  the case of Gel Cloud instances, :gelenv:`INSTANCE` and
  :gelenv:`SECRET_KEY`.
  (More on this in :ref:`the Connection section below
  <ref_intro_clients_connection>`.)
- *Executing queries.* A ``Client`` will provide some methods for executing
  queries against your database. Under the hood, this query is executed using
  Gel's efficient binary protocol.

.. note::

  For some use cases, you may not need a client library. Gel allows you to
  execute :ref:`queries over HTTP <ref_edgeql_http>`. This is slower than the
  binary protocol and lacks support for transactions and rich data types, but
  may be suitable if a client library isn't available for your language of
  choice.

Available libraries
===================

To execute queries from your application code, use one of :ref:`Gel's client
libraries <ref_clients_index>`.

Usage
=====

To follow along with the guide below, first create a new directory and
initialize a project.

.. code-block:: bash

  $ mydir myproject
  $ cd myproject
  $ gel project init

Configure the environment as needed for your preferred language.

.. tabs::

  .. code-tab:: bash
    :caption: Node.js

    $ npm init -y
    $ tsc --init # (TypeScript only)
    $ touch index.ts

  .. code-tab:: bash
    :caption: Deno

    $ touch index.ts

  .. code-tab:: bash
    :caption: Python

    $ python -m venv venv
    $ source venv/bin/activate
    $ touch main.py

  .. code-tab:: bash
    :caption: Rust

    $ cargo init

  .. code-tab:: bash
    :caption: Go

    $ go mod init example/quickstart
    $ touch hello.go

  .. code-tab:: bash
    :caption: .NET

    $ dotnet new console -o . -f net6.0


Install the Gel client library.

.. tabs::

  .. code-tab:: bash
    :caption: Node.js

    $ npm install gel    # npm
    $ yarn add gel       # yarn

  .. code-tab:: txt
    :caption: Deno

    n/a

  .. code-tab:: bash
    :caption: Python

    $ pip install gel

  .. code-tab:: toml
    :caption: Rust

    # Cargo.toml

    [dependencies]
    gel-tokio = "0.5.0"
    # Additional dependency
    tokio = { version = "1.28.1", features = ["macros", "rt-multi-thread"] }

  .. code-tab:: bash
    :caption: Go

    $ go get github.com/geldata/gel-go

  .. code-tab:: bash
    :caption: .NET

    $ dotnet add package Gel.Net.Driver


Copy and paste the following simple script. This script initializes a
``Client`` instance. Clients manage an internal pool of connections to your
database and provide a set of methods for executing queries.

.. note::

  Note that we aren't passing connection information (say, a connection
  URL) when creating a client. The client libraries can detect that
  they are inside a project directory and connect to the project-linked
  instance automatically. For details on configuring connections, refer
  to the :ref:`Connection <ref_intro_clients_connection>` section below.

.. lint-off

.. tabs::

  .. code-tab:: typescript
    :caption: Node.js

    import {createClient} from 'gel';

    const client = createClient();

    client.querySingle(`select random()`).then((result) => {
      console.log(result);
    });


  .. code-tab:: python

    from gel import create_client

    client = create_client()

    result = client.query_single("select random()")
    print(result)

  .. code-tab:: rust

    // src/main.rs
    #[tokio::main]
    async fn main() {
        let conn = gel_tokio::create_client()
            .await
            .expect("Client initiation");
        let val = conn
            .query_required_single::<f64, _>("select random()", &())
            .await
            .expect("Returning value");
        println!("Result: {}", val);
    }

  .. code-tab:: go

    // hello.go
    package main

    import (
      "context"
      "fmt"
      "log"

      "github.com/geldata/gel-go"
    )

    func main() {
      ctx := context.Background()
      client, err := gel.CreateClient(ctx, gel.Options{})
      if err != nil {
        log.Fatal(err)
      }
      defer client.Close()

      var result float64
      err = client.
        QuerySingle(ctx, "select random();", &result)
      if err != nil {
        log.Fatal(err)
      }

      fmt.Println(result)
    }

  .. code-tab:: csharp
    :caption: .NET

    using Gel;

    var client = new GelClient();
    var result = await client.QuerySingleAsync<double>("select random();");
    Console.WriteLine(result);

  .. code-tab:: elixir
    :caption: Elixir

    # lib/gel_quickstart.ex
    defmodule GelQuickstart do
      def run do
        {:ok, client} = Gel.start_link()
        result = Gel.query_single!(client, "select random()")
        IO.inspect(result)
      end
    end

.. lint-on


Finally, execute the file.

.. tabs::

  .. code-tab:: bash
    :caption: Node.js

    $ npx tsx index.ts

  .. code-tab:: bash
    :caption: Deno

    $ deno run --allow-all --unstable index.deno.ts

  .. code-tab:: bash
    :caption: Python

    $ python index.py

  .. code-tab:: bash
    :caption: Rust

    $ cargo run

  .. code-tab:: bash
    :caption: Go

    $ go run .

  .. code-tab:: bash
    :caption: .NET

    $ dotnet run

  .. code-tab:: bash
    :caption: Elixir

    $ mix run -e GelQuickstart.run

You should see a random number get printed to the console. This number was
generated inside your Gel instance using EdgeQL's built-in
:eql:func:`random` function.

.. _ref_intro_clients_connection:

Connection
==========

All client libraries implement a standard protocol for determining how to
connect to your database.

Using projects
--------------

In development, we recommend :ref:`initializing a
project <ref_intro_projects>` in the root of your codebase.

.. code-block:: bash

  $ gel project init

Once the project is initialized, any code that uses an official client library
will automatically connect to the project-linked instance—no need for
environment variables or hard-coded credentials. Follow the :ref:`Using
projects <ref_guide_using_projects>` guide to get started.

Using environment variables
---------------------------

.. _ref_intro_clients_connection_cloud:

For Gel Cloud
^^^^^^^^^^^^^

In production, connection information can be securely passed to the client
library via environment variables. For Gel Cloud instances, the recommended
variables to set are :gelenv:`INSTANCE` and :gelenv:`SECRET_KEY`.

Set :gelenv:`INSTANCE` to ``<org-name>/<instance-name>`` where
``<instance-name>`` is the name you set when you created the Gel Cloud
instance.

If you have not yet created a secret key, you can do so in the Gel Cloud UI
or by running :ref:`ref_cli_gel_cloud_secretkey_create` via the CLI.

For self-hosted instances
^^^^^^^^^^^^^^^^^^^^^^^^^

Most commonly for self-hosted remote instances, you set a value for the
:gelenv:`DSN` environment variable.

.. note::

  If environment variables like :gelenv:`DSN` are defined inside a project
  directory, the environment variables will take precedence.

A DSN is also known as a "connection string" and takes the
following form: :geluri:`<username>:<password>@<hostname>:<port>`.


Each element of the DSN is optional; in fact |geluri| is a technically a
valid DSN. Any unspecified element will default to the following values.

.. list-table::

  * - ``<host>``
    - ``localhost``
  * - ``<port>``
    - ``5656``
  * - ``<user>``
    - |admin|
  * - ``<password>``
    -  ``null``

A typical DSN may look like this:
:geluri:`admin:PASSWORD@db.domain.com:8080`.

DSNs can also contain the following query parameters.

.. list-table::

  * - ``branch``
    - The database branch to connect to within the given instance. Defaults to
      |main|.

  * - ``tls_security``
    - The TLS security mode. Accepts the following values.

      - ``"strict"`` (**default**) — verify certificates and hostnames
      - ``"no_host_verification"`` — verify certificates only
      - ``"insecure"`` — trust self-signed certificates

  * - ``tls_ca_file``
    - A filesystem path pointing to a CA root certificate. This is usually only
      necessary when attempting to connect via TLS to a remote instance with a
      self-signed certificate.

These parameters can be added to any DSN using web-standard query string
notation: :geluri:`user:pass@example.com:8080?branch=my_branch&tls_security=insecure`.


For a more comprehensive guide to DSNs, see the :ref:`DSN Specification
<ref_dsn>`.

Using multiple environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If needed for your deployment pipeline, each element of the DSN can be
specified independently.

- :gelenv:`HOST`
- :gelenv:`PORT`
- :gelenv:`USER`
- :gelenv:`PASSWORD`
- :gelenv:`BRANCH`
- :gelenv:`TLS_CA_FILE`
- :gelenv:`CLIENT_TLS_SECURITY`

.. note::

  If a value for :gelenv:`DSN` is defined, it will override these variables!

Other mechanisms
----------------

:gelenv:`CREDENTIALS_FILE`
  A path to a ``.json`` file containing connection information. In some
  scenarios (including local Docker development) its useful to represent
  connection information with files.

  .. code-block:: json

    {
      "host": "localhost",
      "port": 10700,
      "user": "testuser",
      "password": "testpassword",
      "branch": "main",
      "tls_cert_data": "-----BEGIN CERTIFICATE-----\nabcdef..."
    }

:gelenv:`INSTANCE` (local/Gel Cloud only)
  The name of an instance. Useful only for local or Gel Cloud instances.

  .. note::

      For more on Gel Cloud instances, see the :ref:`Gel Cloud instance
      connection section <ref_intro_clients_connection_cloud>` above.

Reference
---------

These are the most common ways to connect to an instance, however Gel
supports several other options for advanced use cases. For a complete reference
on connection configuration, see :ref:`Reference > Connection Parameters
<ref_reference_connection>`.
