from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime as dt


def veiculoOrganograma(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       ano_inicio_veiculo_diferente_ano_centro_custo=False,
                       dt_inicio_centro_menor_dt_fabricacao_veiculo=False
                       ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    tipoValidacao = "veiculoOrganograma"

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_ano_inicio_veiculo_diferente_ano_centro_custo(pre_validacao):
        nomeValidacao = "Ano de inicio do centro de custo do veiculo diferente do ano do organograma"
        preValidacaoBanco = "ano_inicio_veiculo_diferente_ano_organograma"

        def analisa_ano_inicio_veiculo_diferente_ano_centro_custo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

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

        def corrige_ano_inicio_veiculo_diferente_ano_centro_custo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""
                try:
                    for dados in listDados:
                        mesDia = banco.consultar(f"""SELECT FIRST MAX(RIGHT(hcv.data_inicio, 5)) as mes,i_ano_ccusto from bethadba.hist_cc_veic hcv where hcv.i_veiculos = '{dados['i_chave_dsk1']}' GROUP by hcv.i_ano_ccusto order by mes desc""")

                        busca = banco.consultar(f"""
                                                SELECT v.i_veiculos, 
                                                v.ano as anoVeiculo,
                                                c.i_ano_ccusto as anoCentroCusto,
                                                hcv.data_inicio as anoDataInicio
                                                from bethadba.veiculos v 
                                                join bethadba.hist_cc_veic hcv on (v.i_entidades = hcv.i_entidades and v.i_veiculos = hcv.i_veiculos)
                                                join bethadba.custos c on (hcv.i_custos = c.i_custos and hcv.i_ano_ccusto = c.i_ano_ccusto and hcv.i_entidades = c.i_entidades) 
                                                where v.i_veiculos = '{dados['i_chave_dsk1']}' and hcv.data_inicio = '{dados['i_chave_dsk2']}'
                                                """)
                        if len(busca) > 0:
                            new = f"{busca[0]['anoCentroCusto']}-{mesDia[0]['mes']}"
                            dataInicioFormat = dt.datetime.strftime(busca[0]['anoDataInicio'], "%Y-%m-%d")

                            dadoAlterado.append(f"Alterado a data de inicio do histórico para {new} de CC do veiculo com a placa {dados['i_chave_dsk1']}")
                            comandoUpdate += f"update bethadba.hist_cc_veic set data_inicio = '{new}' where i_veiculos = '{dados['i_chave_dsk1']}' and i_ano_ccusto = {busca[0]['anoCentroCusto']} and data_inicio = '{dataInicioFormat}';\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_ano_inicio_veiculo_diferente_ano_centro_custo {e}")

        if ano_inicio_veiculo_diferente_ano_centro_custo:
            dado = analisa_ano_inicio_veiculo_diferente_ano_centro_custo()

            if corrigirErros and len(dado) > 0:
                corrige_ano_inicio_veiculo_diferente_ano_centro_custo(listDados=dado)

    def valida_corrige_dt_inicio_centro_menor_dt_fabricacao_veiculo(pre_validacao):
        nomeValidacao = "Ano de inicio do centro de custo menor que a data de fabricação"
        preValidacaoBanco = pre_validacao

        def analisa_dt_inicio_centro_menor_dt_fabricacao_veiculo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

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

        def corrige_dt_inicio_centro_menor_dt_fabricacao_veiculo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                try:
                    busca = banco.consultar(f"""
                                            SELECT v.i_veiculos, 
                                            v.data_aquisicao,
                                            v.ano as anoVeiculo,
                                            c.i_ano_ccusto as anoCentroCusto,
                                            hcv.data_inicio as anoDataInicio
                                            from bethadba.veiculos v 
                                            join bethadba.hist_cc_veic hcv on (v.i_entidades = hcv.i_entidades and v.i_veiculos = hcv.i_veiculos)
                                            join bethadba.custos c on (hcv.i_custos = c.i_custos and hcv.i_ano_ccusto = c.i_ano_ccusto and hcv.i_entidades = c.i_entidades) 
                                            where anoVeiculo > LEFT(anoDataInicio, 4)
                                            """)
                    if len(busca) > 0:
                        for i in busca:
                            newAnoVeiculo = i['anoCentroCusto']
                            newDtAquisicao = f"{newAnoVeiculo}-{dt.datetime.strftime(i['data_aquisicao'], '%m-%d')}"

                            dadoAlterado.append(f"Alterado o ano para {newAnoVeiculo} e data de aquisição para {newDtAquisicao} do veiculo {i['i_veiculos']}")

                            comandoUpdate += f"UPDATE bethadba.veiculos set data_aquisicao = '{newDtAquisicao}', ano = '{newAnoVeiculo}' where i_veiculos = '{i['i_veiculos']}';\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_dt_inicio_centro_menor_dt_fabricacao_veiculo {e}")

        if dt_inicio_centro_menor_dt_fabricacao_veiculo:
            dado = analisa_dt_inicio_centro_menor_dt_fabricacao_veiculo()

            if corrigirErros and len(dado) > 0:
                corrige_dt_inicio_centro_menor_dt_fabricacao_veiculo(listDados=dado)

    if dadosList:
        valida_corrige_ano_inicio_veiculo_diferente_ano_centro_custo(pre_validacao='ano_inicio_veiculo_diferente_ano_centro_custo')
        valida_corrige_dt_inicio_centro_menor_dt_fabricacao_veiculo(pre_validacao='dt_inicio_centro_menor_dt_fabricacao_veiculo')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
