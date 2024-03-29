# Pubsub

In this section, we provide guides and references to use the Pubsub connector.

## Requirements

We need to enable the Data Catalog API and use an account with a specific set of minimum permissions:

### Data Catalog API Permissions

- TBD

### GCP Permissions

To execute the metadata extraction workflow successfully, the user or the service account should have enough permissions to fetch required data:

- TBD

Optional permissions, required to fetch policy tags
- TBD


You can visit [this](https://docs.open-metadata.org/connectors/messaging/pubsub/roles) documentation on how you can create a custom role in GCP and assign the above permissions to the role & service account!

You can find further information on the Pubsub connector in the [docs](https://docs.open-metadata.org/connectors/messaging/pubsub).

## Connection Details

$$section
### Host Port $(id="hostPort")

Pubsub APIs URL. By default, the API URL is `pubsub.googleapis.com`. You can modify this if you have custom implementation of Pubsub.
$$

$$section
### GCP Credentials Configuration $(id="gcpConfig")

You can authenticate with your Pubsub instance using either `GCP Credentials Path` where you can specify the file path of the service account key, or you can pass the values directly by choosing the `GCP Credentials Values` from the service account key file.

You can check [this](https://cloud.google.com/iam/docs/keys-create-delete#iam-service-account-keys-create-console) documentation on how to create the service account keys and download it.

$$

$$section
### Taxonomy Project ID $(id="taxonomyProjectID")

BigQuery uses taxonomies to create hierarchical groups of policy tags. To apply access controls to BigQuery columns, tag the columns with policy tags. Learn more about how you can create policy tags and set up column-level access control [here](https://cloud.google.com/bigquery/docs/column-level-security)

If you have attached policy tags to the columns of table available in BigQuery, then OpenMetadata will fetch those tags and attach it to the respective columns.

In this field you need to specify the id of project in which the taxonomy was created.
$$

$$section
### Taxonomy Location $(id="taxonomyLocation")

BigQuery uses taxonomies to create hierarchical groups of policy tags. To apply access controls to BigQuery columns, tag the columns with policy tags. Learn more about how you can create policy tags and set up column-level access control [here](https://cloud.google.com/bigquery/docs/column-level-security)

If you have attached policy tags to the columns of table available in BigQuery, then OpenMetadata will fetch those tags and attach it to the respective columns.

In this field you need to specify the location/region in which the taxonomy was created.
$$
