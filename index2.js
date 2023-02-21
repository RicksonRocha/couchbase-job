const couchbase = require('couchbase')

async function main() {
  const clusterConnStr = 'couchbase://localhost'
  const username = 'Administrator'
  const password = 'Administrator'
  const bucketName = 'shop_customer'

  const cluster = await couchbase.connect(clusterConnStr, {
    username: username,
    password: password,
  })

  const bucket = cluster.bucket(bucketName)
  const scope = bucket.scope('_default')
  const collection = scope.collection('_default')

  const getCustomersByProfession = async (profession) => {
    const query = `SELECT count(*) FROM _default WHERE Profession=$1`
    const options = {parameters: [profession]}
    const queryResult = await scope.query(query, options)
    return queryResult.rows
  }

  const getAvgCustomerAge = async (profession) => {
    let query = "SELECT AVG(`Spending Score (1-100)`) FROM _default"
    const options = {parameters: [profession]}

    if (profession) query+= ` WHERE Profession=$1`

    const queryResult = await scope.query(query, profession ? options : undefined)
    return queryResult.rows
  }

  const getMaiorMenorSalario = async (tipo, profession) => {
    let query = "SELECT CustomerID, "+tipo+"(`Annual Income ($)`) FROM _default GROUP BY CustomerID"
    const options = {parameters: [profession]}

    if (profession) query+= ` WHERE Profession=$1`

    const queryResult = await scope.query(query, profession ? options : undefined)
    return queryResult.rows
  }

  const professions = ['Healthcare', 'Artist']
  // const customers = await getCustomersByProfession(professions[0])
  // const avgAge = await getAvgCustomerAge()
  const maiorSalario = await getMaiorMenorSalario('MAX')

  // console.log('getCustomersByProfession:', professions[0], customers)
  // console.log('getAvgCustomerAge:', professions[0], avgAge)
  console.log('maior salario:', maiorSalario)
}

// Run the main function
main()
  .catch((err) => {
    console.log('ERR:', err)
    process.exit(1)
  })
  .then(process.exit)

  // const user = {
  //   type: 'user',
  //   name: 'Michael',
  //   email: 'michael123@test.com',
  //   interests: ['Swimming', 'Rowing'],
  // }

  // // console.log(queryResult.rows[0]['_default']['Age'])
// queryResult.rows.forEach((row) => {
//   console.log(row['_default']['Age'])
// })

  // Create and store a document
  //   await collection.upsert('michael123', user)

//   const getResults = await Promise.all(
//     users.map((user) => {
//       console.log(`Getting document: ${user}`)
//       return usersCollection.get(user)
//     })
//   )
// getResults.forEach((result) => console.log('Document:', result.content))
