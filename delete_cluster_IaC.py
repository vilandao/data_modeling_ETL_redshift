import boto3
import time
import configparser
from botocore.exceptions import ClientError

redshift_client = boto3.client('redshift', region_name='us-west-2')
iam_client = boto3.client('iam')
ec2_client = boto3.client('ec2', region_name='us-west-2')


def delete_redshift_cluster(config):
    """
    Delete Redshift cluster when no longer use
    """
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier='redshift-cluster-vidao',
            SkipFinalClusterSnapshot=True
        )
    except ClientError as e:
        print(f'ERROR: {e}')
        return None
    else:
        return response['Cluster']


def wait_for_cluster_deletion(cluster_id):
    """
    Print out cluster response info while waiting deletion
    """
    while True:
        try:
            redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        except Exception as e:
            print(e)
            break
        else:
            time.sleep(60)


def delete_iam_role(config):
    """
    Delete IAM role for redshift
    """
    try:
        iam_client.detach_role_policy(
            RoleName=config.get('IAM_ROLE', 'ROLE_NAME'),
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )
        iam_client.delete_role(RoleName=config.get('IAM_ROLE', 'ROLE_NAME'))
    except Exception as e:
        print(e)


def delete_security_group(config):
    """
    Delete redshift security group
    """
    ec2_client.describe_vpcs()
    try:
        ec2_client.delete_security_group(GroupId=config.get('IAM_ROLE', 'SG_ID'))
    except ClientError as e:
        print(e)


def main():
    """
    Initiate redshift cluster deletion
    """

    config = configparser.ConfigParser()
    config.read('../dwh.cfg')

    cluster_info = delete_redshift_cluster(config)

    if cluster_info is not None:
        print(f'Deleting cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Cluster status: {cluster_info["ClusterStatus"]}')

        print('Waiting for cluster to be deleted...')
        wait_for_cluster_deletion(cluster_info['ClusterIdentifier'])
        print('Cluster deleted.')

        delete_iam_role(config)
        print('Role deleted.')

        delete_security_group(config)
        print('Security group deleted.')


if __name__ == '__main__':
    main()
