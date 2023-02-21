const couchbase = require('couchbase')

async function main() {
  const clusterConnStr = 'couchbase://localhost'
  const username = 'Administrator'
  const password = 'Administrator'
  const bucketName = 'german_new_articles'

  const cluster = await couchbase.connect(clusterConnStr, {
    username: username,
    password: password,
  })

  const bucket = cluster.bucket(bucketName)
  const scope = bucket.scope('_default')
  const collection = scope.collection('_default')

  async function getCount(colecao) {
    const query = `SELECT count(*) FROM ${colecao}`
    console.time()
    const queryResult = await scope.query(query)
    console.timeEnd()
    return queryResult.rows
  }

  // const colecao = 'cross_val_split'
  // const count = await getCount(colecao)
  // console.log(`Quantidade de documentos em ${colecao}:`, count)

  // condicional simples
  const getAnnotationsByCategory = async (categoria) => {
    const query = `SELECT count(*) FROM annotations WHERE Category=$1`
    const options = {parameters: [categoria]}
    console.time()
    const queryResult = await scope.query(query, options)
    console.timeEnd()
    return queryResult.rows
  }
  // const categoria = 'PersonalStories'
  // const annotationsByCategory = await getAnnotationsByCategory(categoria)
  // console.log(`Annotations por categoria ${categoria}:`, annotationsByCategory)

  // maior id na tabela de artigos
  const getMaiorIdArtigo = async (profession) => {
    const query = `SELECT MAX(ID_Article) FROM articles`
    console.time()
    const queryResult = await scope.query(query)
    console.timeEnd()
    return queryResult.rows
  }
  // const maiorIdArtigo = await getMaiorIdArtigo()
  // console.log('maior id dos artigos:', maiorIdArtigo);

  // quantidade de posts por categoria
  const getQtdPostsPorCategoria = async () => {
    const query = `SELECT id, Category, COUNT(ID_Post) FROM annotations group by Category, id order by id desc`
    console.time()
    const queryResult = await scope.query(query)
    console.timeEnd()
    return queryResult.rows
  }
  // const qtdPostPorCategoria = await getPostsPorCategoria()
  // console.log('Quantidade de posts por categoria:', qtdPostPorCategoria);

  // maior id com duas tabelas
  async function getMaiorIdPostDuasTabelas() {
    const query = `select max(a.ID_POST) from annotations a inner join categories c on a.Category = c.Name where c.Name = 'SentimentNegative'`
    // const query = 'CREATE INDEX ix2 ON `categories`(`Name`)'
    console.time()
    const queryResult = await scope.query(query)
    console.timeEnd()
    return queryResult.rows
  }
  const maiorIdPostDuasTabelas = await getMaiorIdPostDuasTabelas();
  console.log('Maior id do post com duas tabelas:', maiorIdPostDuasTabelas)

}

// Run the main function
main()
  .catch((err) => {
    console.log('ERR:', err)
    process.exit(1)
  })
  .then(process.exit)