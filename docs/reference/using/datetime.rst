.. _ref_bindings_datetime:

==================
Date/Time Handling
==================

|Gel| has 6 types related to date and time handling:

* :eql:type:`datetime` (:ref:`binary format <ref_protocol_fmt_datetime>`)
* :eql:type:`duration` (:ref:`binary format <ref_protocol_fmt_duration>`)
* :eql:type:`cal::local_datetime` (:ref:`binary format <ref_protocol_fmt_local_datetime>`)
* :eql:type:`cal::local_date` (:ref:`binary format <ref_protocol_fmt_local_date>`)
* :eql:type:`cal::relative_duration` (:ref:`binary format <ref_protocol_fmt_relative_duration>`)
* :eql:type:`cal::date_duration` (:ref:`binary format <ref_protocol_fmt_date_duration>`)

Usually we try to map those types to the respective language-native types, with the following caveats:

* The type is in the standard library
* It covers the range of years (Gel has timestamps from year 1 to 9999)
* It has good enough precision (at least microseconds)

If any of the above criteria is not met, we usually provide a custom type in the client library itself that can be converted to a type from the language's standard library or from a popular third-party library.


Precision
=========

:eql:type:`datetime`, :eql:type:`duration`, :eql:type:`cal::local_datetime` and :eql:type:`cal::relative_duration` all have precision of **1 microsecond**.

This means that if language-native type have a bigger precision such as nanosecond, client library has to round that timestamp when encoding it for |Gel|.

We use **rounding to the nearest even** for that operation. Here are some examples of timestamps with high precision, and how they are stored in the database:

.. code-block:: text

  2022-02-24T05:43:03.123456789Z → 2022-02-24T05:43:03.123457Z
  2022-02-24T05:43:03.000002345Z → 2022-02-24T05:43:03.000002Z
  2022-02-24T05:43:03.000002500Z → 2022-02-24T05:43:03.000002Z
  2022-02-24T05:43:03.000002501Z → 2022-02-24T05:43:03.000003Z
  2022-02-24T05:43:03.000002499Z → 2022-02-24T05:43:03.000002Z
  2022-02-24T05:43:03.000001234Z → 2022-02-24T05:43:03.000001Z
  2022-02-24T05:43:03.000001500Z → 2022-02-24T05:43:03.000002Z
  2022-02-24T05:43:03.000001501Z → 2022-02-24T05:43:03.000002Z
  2022-02-24T05:43:03.000001499Z → 2022-02-24T05:43:03.000001Z

.. note::

  A quick refresher on rounding types: If we perform multiple operations of summing while rounding half-up or rounding half-down, the error margin of the resulting value tends to increase. If we round half-to-even instead, the expected value of summing tends to be more accurate.

Note as described in :ref:`datetime protocol documentation <ref_protocol_fmt_datetime>` the value is encoded as a *signed* microseconds delta since a fixed time. Some care must be taken when rounding negative microsecond values.

Rounding to the nearest even applies to all operations that client libraries perform, in particular:

1. Encoding timestamps *and* time deltas (see the :ref:`list of types <ref_bindings_datetime>`) to the binary format if precision of the native type is higher than microseconds.
2. Decoding timestamps *and* time deltas from the binary format is precision of native type is lower than microseconds (applies for JavaScript for example)
3. Converting from Gel specific type (if there is one) to native type and back (depending on the difference in precision)
4. Parsing a string to a Gel specific type (this operation is optional to implement, but if it is implemented, it must obey the rules)
