-- PROJETO FINAL SCC0641_Turma01_2Sem_2023_ET --

> Autores:
* Danielle Modesti - NUSP: 12543544
* Hélio Nogueira Cardoso - NUSP: 10310227

> Requisitos:
* python3
* pip3
* Para instalar os pacotes externos do Python:
$ pip install -r requirements.txt

* Instalação do driver da oracle:
    * Oracle over Python: https://www.oracle.com/database/technologies/appdev/python/quickstartpythononprem.html#linux-tab
    * Download Instant Client Basic Light:
        * Linux: https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basiclite-linuxx64.rpm
        * Windows: https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
        * MacOS: https://www.oracle.com/database/technologies/instant-client/macos-intel-x86-downloads.html
    * Linux:
    $ sudo alien -i oracle-instantclient-basiclite-linuxx64.rpm

* IMPORTANTE: 
    * Preencha o arquivo '.env' com as informações da sua conexão oracle.
    * Coloque também no '.env' o caminho do diretório em que se encontra o drive oracle instalado
    * OBS.: No linux, o caminho costuma ser por padrão "/usr/lib/oracle/21/client64/lib/"

* EXECUÇÃO:
$ python3 main.py

* FUNCIONALIDADES:
    * Funcionalidades de 1 a 6:
        * Estas funcionalidades irão criar no diretório corrente um arquivo 'mongo.mql'
        * Este arquivo irá conter o código MQL adequado para a inserção ou criação de índice pedida
        * Em um terminal mongosh a partir do mesmo diretório, execute 'load('mongo.mql')' para executar no mongodb
    * Funcionalidade 7:
        * Para esta funcionalidade pode ser passada qualquer consulta SELECT nos formatos pedidos nas questões 4, 5 e 6.
        * As 6 funções de agregação disponíveis são:
            * MIN
            * MAX
            * AVG
            * SUM
            * COUNT
            * MEDIAN (OBS: $median só funciona no MongoDB a partir da versão 7)
        * Em consultas com junção:
            * Você pode utilizar o nome das tabelas para diferenciar os campos de uma tabela ou de outra
            * Isto serve apenas para que a mesma consulta que funciona no Oracle possa ser copiada e colada no programa
            * Contudo, o programa irá assumir na condição de junção que o primeiro atributo é primeira tabela, análoga para a segunda
            * IMPORTANTE: não inclua ';' na sua consulta do programa

* COBERTURA DAS QUESTÕES:
    * Questão 1: coberta pelas funcionalidades 1, 2, 3 e 4
    * Questão 2: coberta pelas funcionalidades 5 e 6
    * Questão 3: escrita manualmente em arquivo 'questao_3_validators.mql'
    * Questões 4, 5 e 6: cobertas pela funcionalidade 7