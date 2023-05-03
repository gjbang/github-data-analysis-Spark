db.createUser(
    {
        user: 'spark',
        pwd: 'msbd5003',
        roles: ["readWrite", "dbOwner"]
    }
)