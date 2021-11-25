# 2.0.1

---

- Minor fix: change column status to visible = true for browse codes 100 and 200.
- change in logging for twinfield tests


# 2.0.0

---

- major release of twinfield package, including updates to code structure.


# 1.0.9

---

- Changes for module 100 and 200: the batches in which the requests are made have been reduced in size. By this release, the data is pulled for each period, but also halved in size by splitting the request in `debit` and `credit` transactions.
- Renamed variable `periods` to `batches`


# 1.0.8

---

- Minor fixes

# 1.0.7

---

- Set mutations query to periods of months.

# 1.0.6

---

- Added insert method to update or insert transactions in Twinfield.
