# Open Access Website Workflow
A workflow that generates the data for the COKI Open Access Website (still in progress), copies the data to the website
static folder, builds the website and then deploys it to Cloudflare.

+------------------------------+--------------------------------------+
| Summary                      |                                      |
+==============================+======================================+
| Frequency                    | Default: @weekly                     |
+------------------------------+--------------------------------------+
| Runs on remote worker        | Default: False                       |
+------------------------------+--------------------------------------+
| Catchup missed runs          | Default: False                       |
+------------------------------+--------------------------------------+
| Credentials Required         | Yes                                  |
+------------------------------+--------------------------------------+
| Uses Workflow Template       | Workflow                             |
+------------------------------+--------------------------------------+

## Airflow connections
In the config.yaml file, the following airflow connections are required.

### cloudflare_api_token
This connection contains Cloudflare API token that enables Wrangler to publish the Open Access Website to Cloudflare.
See [Cloudflare Creating API tokens](https://developers.cloudflare.com/api/tokens/create) for instructions on how to 
create a Cloudflare API token. The Cloudflare API token is already URL encoded. The required settings for the token are 
listed below. 

Permissions:
* Account, Workers KV Storage:Edit
* Account, Workers Scripts:Edit
* Account, Account Settings:Read
* User, User Details:Read
* Zone, Workers Routes: Edit

Account Resources:
* Include, <select your account>

Zone Resources:
* Include, Specific zone: <select the domain you will deploy to>

```yaml
cloudflare_api_token: http://:<cloudflare_api_token>@
```

## External references
* [Cloudflare Creating API tokens](https://developers.cloudflare.com/api/tokens/create)
