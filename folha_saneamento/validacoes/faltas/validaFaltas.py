from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import datetime


def faltas(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
           corrigirErros=False,
           falta_concomitante_dt_afastamento=False,
           falta_concomitante_dt_ferias=False
           ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_falta_concomitante_dt_afastamento(pre_validacao):
        nomeValidacao = "Data inicial da falta concomitante com afastamentos"

        def analisa_falta_concomitante_dt_afastamento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_falta_concomitante_dt_afastamento(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select a.i_entidades,
                                            a.i_funcionarios,
                                            a.i_faltas,
                                            a.dt_inicial,
                                            b.dt_afastamento
                                            from bethadba.faltas a 
                                            join bethadba.afastamentos b on  a.i_entidades = b.i_entidades and a.i_funcionarios = b.i_funcionarios 
                                            where a.dt_inicial  BETWEEN b.dt_afastamento  and b.dt_ultimo_dia
                                        """)

                for row in dados:
                    newDtInicial = row['dt_afastamento']
                    newDtInicial -= datetime.timedelta(days=1)

                    dadoAlterado.append(f"Alterado a data inicial da falta {row['i_faltas']} de {row['dt_inicial']} para {newDtInicial} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                    comandoUpdate += f"""UPDATE bethadba.faltas set dt_inicial = '{newDtInicial}' 
                                            where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_faltas = {row['i_faltas']} and dt_inicial = '{row['dt_inicial']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_falta_concomitante_dt_afastamento: {e}")
            return

        if falta_concomitante_dt_afastamento:
            dado = analisa_falta_concomitante_dt_afastamento()

            if corrigirErros and len(dado) > 0:
                corrige_falta_concomitante_dt_afastamento(listDados=dado)

    def analisa_corrige_falta_concomitante_dt_ferias(pre_validacao):
        nomeValidacao = "Data inicial da falta concomitante com férias"

        def analisa_falta_concomitante_dt_ferias():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_falta_concomitante_dt_ferias(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            select 
                                            a.i_funcionarios,
                                            a.dt_inicial,
                                            b.dt_gozo_ini
                                            from bethadba.faltas a 
                                            join bethadba.ferias b on a.i_entidades = b.i_entidades  and a.i_funcionarios = b.i_funcionarios  
                                            where a.dt_inicial  BETWEEN b.dt_gozo_ini  and b.dt_gozo_fin
                                        """)

                for row in dados:
                    newDtInicial = row['dt_gozo_ini']
                    newDtInicial -= datetime.timedelta(days=1)

                    dadoAlterado.append(f"Alterado a data inicial da falta {row['i_faltas']} de {row['dt_inicial']} para {newDtInicial} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                    comandoUpdate += f"""UPDATE bethadba.faltas set dt_inicial = '{newDtInicial}' 
                                            where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_faltas = {row['i_faltas']} and dt_inicial = '{row['dt_inicial']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_falta_concomitante_dt_ferias: {e}")
            return

        if falta_concomitante_dt_ferias:
            dado = analisa_falta_concomitante_dt_ferias()

            if corrigirErros and len(dado) > 0:
                corrige_falta_concomitante_dt_ferias(listDados=dado)

    if dadosList:
        analisa_corrige_falta_concomitante_dt_afastamento(pre_validacao="falta_concomitante_dt_afastamento")
        analisa_corrige_falta_concomitante_dt_ferias(pre_validacao="falta_concomitante_dt_ferias")
