import clickhouse_connect

if __name__ == '__main__':
    client = clickhouse_connect.get_client(
        host='pr76dav40j.ap-south-1.aws.clickhouse.cloud',
        user='default',
        password='Px.J1Nha_SrXr',
        secure=True
    )
    print("Result:", client.query("SELECT 1").result_set[0][0])