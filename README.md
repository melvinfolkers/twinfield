<img src="https://www.zypp.io/static/assets/img/logos/Main logo - White/Zypp - White - PNG 250px.png" width="200">

# twinfield


> Repository for downloading twinfield data to a azure database
The repository currently supports the import of the following modules:

<b>Browse codes</b>
1. 030_1 (transactions)
2. 040_1 (annual report)
3. 100 (Customers)
4. 200 (Suppliers) 

<b>Dimensions</b>
1. dimensions_kpl (kostenplaatsen)
2. dimensions_deb (debiteuren stamgegevens)
3. dimensions_crd (crediteuren stamgegevens)

## Usage

`from twinfield import twinfield` 
<br><br>
`df = twinfield.query("040_1",jaar =2020)`

## Environments variables
some credentials are needed for connecting with the twinfield API.Add the following credentials to the environment variables.<br> 
```
TW_USER_LS=""
TW_PW_LS=""
TW_ORG_LS=""
```