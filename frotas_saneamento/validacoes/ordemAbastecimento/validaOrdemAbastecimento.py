from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime as dt


def ordemAbastecimento(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       combustivel_ordem_diferente_combustivel_veiculo=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    tipoValidacao = "ordemAbastecimento"

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_combustivel_ordem_diferente_combustivel_veiculo(pre_validacao):
        nomeValidacao = "Combustivel da ordem de abastecimento diferente do combustivel padrão do veiculo"
        preValidacaoBanco = pre_validacao

        def analisa_combustivel_ordem_diferente_combustivel_veiculo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}'")

            dados = []

            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_combustivel_ordem_diferente_combustivel_veiculo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                SELECT first o.i_veiculos as ordemVeiculo,
                                                o.i_materiais as ordemMateriais,
                                                v.i_materiais as veiculoMaterial
                                                from bethadba.ordens o
                                                join bethadba.veiculos v on (v.i_veiculos = o.i_veiculos)
                                                where o.i_ordens = {dados['i_chave_dsk1']} and v.i_materiais != o.i_materiais
                                                """)
                        if len(busca) > 0:

                            dadoAlterado.append(f"Alterado o combustivel da ordem {dados['i_chave_dsk1']} para {busca[0]['veiculoMaterial']}")

                            comandoUpdate += f"update bethadba.ordens set i_materiais = {busca[0]['veiculoMaterial']} where i_ordens = {busca[0]['ordemVeiculo']};\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_combustivel_ordem_diferente_combustivel_veiculo {e}")

        if combustivel_ordem_diferente_combustivel_veiculo:
            dado = analisa_combustivel_ordem_diferente_combustivel_veiculo()

            if corrigirErros and len(dado) > 0:
                corrige_combustivel_ordem_diferente_combustivel_veiculo(listDados=dado)

    if dadosList:
        valida_corrige_combustivel_ordem_diferente_combustivel_veiculo(pre_validacao='combustivel_ordem_diferente_combustivel_veiculo')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
