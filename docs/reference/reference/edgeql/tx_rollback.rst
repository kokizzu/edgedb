..
    Portions Copyright (c) 2019 MagicStack Inc. and the Gel authors.

    Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
    Portions Copyright (c) 1994, The Regents of the University of California

    Permission to use, copy, modify, and distribute this software and its
    documentation for any purpose, without fee, and without a written agreement
    is hereby granted, provided that the above copyright notice and this
    paragraph and the following two paragraphs appear in all copies.

    IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
    DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
    LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
    DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.

    THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
    INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
    AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
    ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
    PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.


.. _ref_eql_statements_rollback_tx:

Rollback
========

:eql-statement:


``rollback`` -- abort the current transaction

.. eql:synopsis::

    rollback ;


Example
-------

Abort the current transaction:

.. code-block:: edgeql

    rollback;


Description
-----------

The ``rollback`` command rolls back the current transaction and causes all
updates made by the transaction to be discarded.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > EdgeQL > Start transaction
      <ref_eql_statements_start_tx>`
  * - :ref:`Reference > EdgeQL > Commit
      <ref_eql_statements_commit_tx>`
  * - :ref:`Reference > EdgeQL > Declare savepoint
      <ref_eql_statements_declare_savepoint>`
  * - :ref:`Reference > EdgeQL > Rollback to savepoint
      <ref_eql_statements_rollback_savepoint>`
  * - :ref:`Reference > EdgeQL > Release savepoint
      <ref_eql_statements_release_savepoint>`
