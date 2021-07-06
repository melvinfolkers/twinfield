<p align="center"><img alt="logo" src="https://www.zypp.io/static/assets/img/logos/zypp/white/500px.png" width="200"></p>

# Twinfield

---
> Python package for reading and insert data in Twinfield using the official Twinfield API.

## Installation

---
```commandline
pip install twinfield
```

## Scoping

---
This package currently supports the import of the following modules:

<b>Browse codes</b>
1. 030_1 (Transactions)
2. 040_1 (Annual report)
3. 100 (Customers)
4. 200 (Suppliers) 

<b>Dimensions</b>
1. dimensions_kpl (kostenplaatsen)
2. dimensions_deb (debiteuren stamgegevens)
3. dimensions_crd (crediteuren stamgegevens)

More modules will be added in the near future.

## Usage

---
`from twinfield import twinfield` 
<br><br>
`df = twinfield.query("040_1", jaar=2020)`

## Environment variables

---
Some credentials are needed for connecting with the twinfield API. Add the following credentials to your environment variables.<br> 
```
TW_USER_LS=""
TW_PW_LS=""
TW_ORG_LS=""
```
How to set environment variables?
- [Windows](https://stackoverflow.com/questions/5898131/set-a-persistent-environment-variable-from-cmd-exe)
- [macOS](https://apple.stackexchange.com/questions/106778/how-do-i-set-environment-variables-on-os-x)
- [Linux](https://unix.stackexchange.com/questions/117467/how-to-permanently-set-environmental-variables)

---

[Link](https://www.twinfield.nl/api) to the Twinfield API documentation.

---

For business inquiries concerning this package, contact us at:
- melvin@zypp.io
- erfan@zypp.io