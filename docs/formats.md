# Formats

Everything on disk is the binary form from kolo-log: the header
(`Header().Encode()`), the latest head (`Head.Encode()`) and, for each
entry, its record and either its ciphertext or, once deleted, its signed
delete marker, in an entry value of version 3. A live entry's key is stored
on its own, 32 bytes under a key that sorts by index apart from the
entries, so it is read and checked without the ciphertext. A replica is stored
the same way, without a head until an update reaches one, and its list key
holds one byte (1) where an own log's is empty. During a catch-up a
replica also stores a target value: the head it is filling and the
consistency proof from its stored entries to it. There is no JSON on disk.

Each log lives under its own key prefix, so reading, listing or deleting
one log never touches another. Entry keys use a fixed-width big-endian index
and sort by index. A custom `Backend` only needs to store keys and values as
given and scan them in bytewise order.

## References

- [RFC 9162, Certificate Transparency Version 2.0](https://www.rfc-editor.org/info/rfc9162) (Experimental, December 2021).
  Takes: section 8.2, the monitor that keeps every entry, which a stored replica follows.
  Not implemented: timestamps in heads, so heads are ordered by size only.
- [NIST SP 800-88 Rev. 1](https://doi.org/10.6028/NIST.SP.800-88r1) (December 2014), section 2.6, Cryptographic Erase.
  Takes: deleting by destroying a key.
  Not implemented: key wrapping by a master key; the key is stored in the clear next to the ciphertext, because the store trusts the node's own disk.
- Reardon, Basin and Capkun, [SoK: Secure Data Deletion](https://doi.org/10.1109/SP.2013.28), IEEE S&P 2013.
  Takes: why a log-structured engine keeps old bytes until it compacts.
- O'Neil et al., [The Log-Structured Merge-Tree](https://doi.org/10.1007/s002360050048), Acta Informatica 1996.
  Takes: the storage model of the embedded storage engine, where a delete writes a tombstone.
