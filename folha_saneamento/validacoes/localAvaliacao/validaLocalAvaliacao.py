from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import colorama


def localAvaliacao(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   numero_sala_nulo=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_numero_sala_nulo(pre_validacao):
        nomeValidacao = "Numero da sala não informado"

        def analisa_numero_sala_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_numero_sala_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select i_pessoas, i_locais_aval, descricao
                                            from bethadba.locais_aval
                                            where num_sala is null or num_sala = ' ';
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Inserido o numero da sala 1 no local de avaliação {row['i_locais_aval']}-{row['descricao']}")
                        comandoUpdate += f"""UPDATE bethadba.locais_aval set num_sala = 1 where i_pessoas = {row['i_pessoas']} and i_locais_aval = {row['i_locais_aval']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_numero_sala_nulo: {e}")
            return

        if numero_sala_nulo:
            dado = analisa_numero_sala_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_numero_sala_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_numero_sala_nulo(pre_validacao="numero_sala_nulo")
