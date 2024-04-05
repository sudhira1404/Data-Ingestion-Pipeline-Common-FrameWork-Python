"""

#Get access token from vault using the nuid svmdedmp/svmdfpwv ,key in its password:
​
client_token=`curl https://prod.vault.target.com/v1/auth/ldap/login/${nuid} -X POST --data '{"password":""}' | jq -r .auth.client_token`
​
#the token valid for 30 mins​
​
#access the vault using the token to get the password.To get password for svmdedmp,password is stored in  mdf_file_ingester/svmdedmp_nuid in vault
similarly for other nuid's ,password will be stored in mdf_file_ingester/${nuid}_nuid path in vault.If password is not there,then first we need store it in vault
in the path mdf_file_ingester/${nuid}_nuid path

curl https://prod.vault.target.com/v1/secret/mdf_file_ingester/svmdedmp_nuid -H 'X-Vault-Token: '"${client_token}"''
or
curl https://prod.vault.target.com/v1/secret/mdf_file_ingester/svmdedmp_nuid -H 'X-Vault-Token: '"${client_token}"'' | jq -r .data.password
​
#to Store  password in the vault for eg nuid svmdedmp.Password should be stored in the path  mdf_file_ingester/${nuid}_nuid in the vault.

curl https://prod.vault.target.com/v1/secret/mdf_file_ingester/svmdedmp_nuid -H 'X-Vault-Token: '"${client_token}"'' -H "Content-Type: application/json" -X POST --data '{"password":""}'

Note:if we overwrite the vault path all the previous values are lost.So we making sure to store one key value pair that is password: {$password} for nuid in seperate path
like mdf_file_ingester/${nuid}_nuid

to list:

curl https://prod.vault.target.com/v1/secret/mdf_file_ingester/svmdedmp_nuid -H 'X-Vault-Token: '"${client_token}"'' -X LIST
​

"""
import logging


def pwv(user_id,password,vault_password_path):
    import requests
    user_id = user_id
    password = password
    vault_password_path=vault_password_path
    token_url = f'https://prod.vault.target.com/v1/auth/ldap/login/{user_id}'
    data = {
        "password": password
    }

    token_response = requests.post(token_url, json=data, verify=False)
    client_token = token_response.json()["auth"]["client_token"]
    secret_url = f'https://prod.vault.target.com/v1/secret/{vault_password_path}'
    header = {
        "X-Vault-Token": client_token,
        "Content-Type": "application/json"
    }
    secret_response = requests.get(secret_url, headers=header, verify=False)
    print(secret_response.json()["data"]["password"])


def hvac_pwv_read(user_id,password,vault_addr,vault_password_path,secrets_key=[]):
    import os
    import hvac
    import urllib.request
    import tempfile
    bundleFile = os.path.join(tempfile.gettempdir(), "tgt-ca-bundle.crt")
    urllib.request.urlretrieve("http://browserconfig.target.com/tgt-certs/tgt-ca-bundle.crt", bundleFile)
    os.environ['REQUESTS_CA_BUNDLE'] = bundleFile
    uid = user_id
    pwd = password
    VAULT_ADDR=vault_addr
    vault = hvac.Client(url=VAULT_ADDR,verify=False)
    vault.auth.ldap.login(username=uid, password=pwd)
    retrieved_secrets = {}
    try:
        dict_of_secrets_data = vault.read(vault_password_path)
        dict_of_secrets = dict_of_secrets_data["data"]
    except Exception:
        return retrieved_secrets
    #return dict_of_secrets["data"]
    if not secrets_key:
        return dict_of_secrets
    try:
        for secrets_key1 in secrets_key:
            data = {secrets_key1: dict_of_secrets[secrets_key1]}
            retrieved_secrets.update(data)
    except KeyError as error:
        logging.info(str(error))
        logging.error("Exiting with error %s while updating key %s" % (error,data))
    return retrieved_secrets


def hvac_pwv_write(user_id, password,vault_addr,vault_password_path,secrets):
    import os
    import hvac
    import urllib.request
    import tempfile
    bundleFile = os.path.join(tempfile.gettempdir(), "tgt-ca-bundle.crt")
    urllib.request.urlretrieve("http://browserconfig.target.com/tgt-certs/tgt-ca-bundle.crt", bundleFile)
    os.environ['REQUESTS_CA_BUNDLE'] = bundleFile
    uid = user_id
    pwd = password
    VAULT_ADDR = vault_addr
    vault = hvac.Client(url=VAULT_ADDR, verify=False)
    vault.auth.ldap.login(username=uid, password=pwd)
    logging.info("Client authenticated = [%s].  Writing personal secrets in the path %s." % (vault.is_authenticated(),vault_password_path))
    """
    write_password_escaped = write_password.translate(str.maketrans({'"': r'\"',
                                                                     "\\": r"\\",
                                                                     "/": r"\/"}))
    vault.write(vault_password_path, password=write_password_escaped)
    mySecrets = vault.read(vault_password_path)
    return mySecrets["data"]['password']
    """
    derived_secrets = secrets
    for key, value in derived_secrets.items():
        value.translate(str.maketrans({'"': r'\"',
                                         "\\": r"\\",
                                         "/": r"\/"}))
        derived_secrets[key] = value
    tot_secrets = hvac_pwv_read(user_id,password,vault_addr,vault_password_path)
    if tot_secrets:
        tot_secrets.update(derived_secrets)
    else:
        tot_secrets = secrets
    vault.write(vault_password_path, **tot_secrets)
    #list_of_secrets = vault.read(vault_password_path)
    #print(list_of_secrets["data"])

#Read password for nuid.Specify list of nuid's

list_of_nuid = ['svmdedmp','svmdfghe', 'svhkyanp',
        'svhkyans', 'svhkyhds', 'svhkyhdp',
        'svaffdmp','svaffdmd', 'svsgnhds',
        'svmdedmd','svmdfpwv'
        ]

#retrieved_secrets=hvac_pwv_read('svmdfpwv','DCgh730836875$#','https://prod.vault.target.com','secret/mdf_file_ingester/nuid',list_of_nuid)
#print(retrieved_secrets)
#print(retrieved_secrets['svmdedmp'])

#Write password or nuid.Specify nuid:password as key value pair

dict_of_nuid_password = {'svmdedmp': 'esr@18lr6583264', 'svmdfghe': 'IHbc263261166(*',
                             'svhkyanp': 'FPzu3j8sZvK6^VN',
                             'svhkyans': 'bR7w^A@8D&5UKfa', 'svhkyhds': 'wq27$r#LQA8JQ33',
                             'svhkyhdp': 'CCZ(qap&]<uL2Di',
                             'svaffdmp': 'osrn69:OMguhfyq', 'svaffdmd': 'yUKZ23#qgnhpzfl',
                             'svsgnhds': 'GBdi794730274&@',
                             'svmdedmd': 'XaUkHz8WFzc#N#t','svmdfpwv': 'DCgh730836875$#'
                             }
#hvac_pwv_write('svmdfpwv','DCgh730836875$#','https://prod.vault.target.com','secret/mdf_file_ingester/nuid',dict_of_nuid_password)

xenon_user='svmdedmd'
retrieved_secrets=hvac_pwv_read('svmdfpwv','DCgh730836875$#','https://prod.vault.target.com','secret/mdf_file_ingester/nuid',list(xenon_user.split(" ")))
print(retrieved_secrets)
print(retrieved_secrets[xenon_user])
