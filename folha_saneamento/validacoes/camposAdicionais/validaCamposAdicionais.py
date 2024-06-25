from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def camposAdicionais(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 duplicidade_descricao_caracteristica=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_duplicidade_descricao_caracteristica(pre_validacao):
        nomeValidacao = "Busca os campos adicionais com descrição repetido"

        def analisa_duplicidade_descricao_caracteristica():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_duplicidade_descricao_caracteristica(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        list(i_caracteristicas) as caracteri, 
                        TRIM(nome) as nomes, 
                        count(nome) 
                    FROM 
                        bethadba.caracteristicas 
                    GROUP BY 
                        nomes
                    HAVING 
                        count(nome) > 1   
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        list_caracteristicas = row['caracteri'].split(',')
                        for caracteristica in list_caracteristicas:
                            novo_nome = row['nomes'] + ' ' + caracteristica
                            dadoAlterado.append(f"Alterado nome da caracteristica {caracteristica} de {row['nomes']} para {novo_nome}")
                            comandoUpdate += f"""UPDATE bethadba.caracteristicas set nome = '{novo_nome}' where i_caracteristicas = {caracteristica};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_duplicidade_descricao_caracteristica: {e}")
            return

        if duplicidade_descricao_caracteristica:
            dado = analisa_duplicidade_descricao_caracteristica()

            if corrigirErros and len(dado) > 0:
                corrige_duplicidade_descricao_caracteristica(listDados=dado)

    if dadosList:
        analisa_corrige_duplicidade_descricao_caracteristica(pre_validacao="duplicidade_descricao_caracteristica")
