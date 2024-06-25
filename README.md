<div align="center">
<h1 align="center">Saneamento Automático</h1>
</div>

<div>
Este é um projeto desenvolvido com o intuito de realizar o saneamento de bases desktop para migração em <b>homologação</b>.<br>

O Projeto é dividido em pastas, na qual, cada pasta é para um sistema desktop, sempre com a nomenclatura "{nome_sistema}_
saneamento", e em cada pasta existe três elementos principais:<br>
<ul>
<li><b>populandoTabela</b>: Pasta que contem informações para inserir uma chave na tabela <b>bethadba.controle_migracao_registro_ocor</b>, para que o projeto consiga identificar separadamente cada inconsistência,</li>
<li><b>Validações</b>: Local onde se encontra todas as validações. Cada validação é separada por uma pasta com seu respectivo nome e um arquivo .py, na qual contém as correções. Esse arquivo .py sempre seguirá um padrão de nomenclatura "valida{nome_validacao}.py" afim de facilitar a manutenções e aplicação de novas correções.</li>
<li><b>main{nome_sistema}.py</b>: Esse arquivo .py é responsável por toda a execução do projeto para o respectivo sistema selecionado, nele contém as informações cruciais para a execução, dentre elas, o nome da conexão ODBC, as informações para conexão do banco postgres e a chamada de cada validação presente na pasta <b>Validações</b>.</li>
</ul>
<br>

Além disso o projeto contém a pasta <b>Utilitarios</b>, onde fica localizado as principais funcionalidades para o
funcionamento do projeto. Sendo elas:
<ul>
<li><b>bancoDeAlteracoes</b>: Pasta que contém o arquivo "logAlteracoes.py", responsável pela criação da tabela de registros alterados no banco Postgres.</li>
<li><b>coletaDados</b>: Pasta que contém o arquivo "tabelaControle.py", utilizado para coletar os erros na tabela <b>bethadba.controle_migracao_registro_ocor</b>, com a respectiva chave inserida através da pasta <b>populandoTabela</b></li>
<li><b>conexao</b>: Pasta que contém o arquivo "conectaOdbc.py", responsável pela conexão com a ODBC informada no main de cada sistema e, com funções para consultar e executar comandos SQL</li>
<li><b>funcoesGenericas</b>: Pasta que contém o arquivo "funcoes.py", na qual é um arquivo que possui diversas funções utilizadas no projeto.</li>
<li><b>logExecucao</b>: Pasta que contém o arquivo "funcoesLogExecucao.py", que cria um arquivo txt que salva conteudos apresentados no log de execução do projeto.</li>
<li><b>popularTabelaControle</b>: Pasta que contém o arquivo "popularTabelaControle.py", responsável por inserir as chaves das inconsistências de cada sistema na tabela <b>bethadba.controle_migracao_registro_ocor</b></li>
</ul>
</div>
<br>
<br>
<div>
<h2> Como configurar o ambiente para execução do projeto </h2>

1. Configure o ambiente virtual (recomendado):
   ```
   python -m venv venv
   ```

2. Ative o ambiente virtual:
    ```
    venv\Scripts\activate
    ```

3. Instale as dependências:
    ```
    pip install -r requirements
    ```

4. Crie um banco de dados **Postgres**


5. Dentro do arquivo **main** de cada sistema, inserir os dados do banco criado na classe **LogAlteracoes**.


6. Criar conexão ODBC de preferência com usuário e senha bethadba. <br>
   OBS: Pode ser necessário em algumas validações o uso do token **BETHADBA** na qual é fornecida semanalmente, pois alguns comandos SQL utilizam funções que podem ser restritas a usuários Bethadba.


7. Na variável **nomeOdbc** localizada no **main** de cada sistema, inserir o nome da conexão criada no passo **6**


8. Executar o arqjob de pre-validação para que a tabela <b>bethadba.controle_migracao_registro_ocor</b> seja alimentada e posteriormente utilzada pelo projeto.


9. Executar o **main** que desejar e acompanhar as correções no log de execução.

<br>
<br>
<h3> Alguns detalhes a serem feitos na versão atual do projeto </h3>
Na base que estiver utilizando, crie uma conexão ODBC com usuário e senha <b>bethadba</b> e, token <b>BETHADBA</b>, como descrito na etapa <b>6</b>.<br>
Após isso, acessar o Interactive SQL, informar a conexão criada e executar o comando abaixo:

```
call bethadba.pg_habilitartriggers('off');
ALTER TABLE bethadba.controle_migracao_registro_ocor ADD pre_validacao varchar(100) NULL;
commit;
```
Para que seja criada a coluna responsável pela identificação de cada inconsistência na tabela <b>bethadba.controle_migracao_registro_ocor</b>.


</div>
