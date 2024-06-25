from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import colorama
import datetime


def lancamentoEvento(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                     corrigirErros=False,
                     lancamento_faltante=False,
                     lancamento_posterior_rescisao=False,
                     lancamento_posterior_cessacao=False
                     ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_lancamento_faltante(pre_validacao):
        nomeValidacao = "Lançamento faltante."

        def analisa_lancamento_faltante():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_lancamento_faltante(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select 
                                             v.i_entidades,
                                             v.i_funcionarios,
                                             v.i_eventos,
                                             v.i_tipos_proc,
                                             v.i_processamentos,
                                             isnull(count((hs.dt_alteracoes)),0) as quantidade,
                                             v.dt_inicial,
                                             v.dt_final,
                                             mes = if DATEDIFF(month, v.dt_inicial, v.dt_final) = 0 then 
                                                          if v.dt_inicial = v.dt_final then
                                                                   1
                                                           else 0 endif                                   
                                                     else
                                                         DATEDIFF(month, v.dt_inicial, v.dt_final)
                                                     endif
                                            from bethadba.variaveis v 
                                            join bethadba.hist_salariais hs on v.i_entidades = hs.i_entidades and v.i_funcionarios = hs.i_funcionarios
                                            join bethadba.funcionarios f on hs.i_entidades = f.i_entidades and hs.i_funcionarios = f.i_funcionarios 
                                            where isnull(date(hs.dt_alteracoes),GETDATE()) BETWEEN v.dt_inicial and v.dt_final 
                                            and f.tipo_func = 'A' and f.conselheiro_tutelar = 'N'
                                            group by hs.i_entidades, hs.i_funcionarios,v.dt_inicial, v.dt_final, v.i_funcionarios, v.i_eventos, v.i_tipos_proc, v.i_processamentos, v.i_entidades
                                            having quantidade < mes
                                            order by v.i_entidades,v.i_funcionarios,v.i_eventos, v.dt_inicial
                                        """)

                for row in dados:
                    newDtFinalLancamento = ""

                    entidade = row['i_entidades']
                    funcionario = row['i_funcionarios']
                    evento = row['i_eventos']
                    tipoProc = row['i_tipos_proc']
                    processamento = row['i_processamentos']
                    dtInicial = row['dt_inicial']
                    dtFinal = row['dt_final']

                    # coletando o proximo lançamento de variavel
                    nextLancamento = banco.consultar(f"""SELECT first v.dt_inicial
                                                            from bethadba.variaveis v
                                                            where v.i_entidades = {entidade} and v.i_funcionarios = {funcionario} and v.i_eventos = {evento} and i_tipos_proc = {tipoProc}
                                                            and dt_inicial > '{dtInicial}'
                                                            order by dt_inicial
                                                    """)

                    # caso não retorne algum lançamento posterior ao lançamento incorreto, busca a ultima alteração salarial
                    if len(nextLancamento) == 0:
                        lastHistSalarial = banco.consultar(f"""SELECT first i_entidades, i_funcionarios, cast(dt_alteracoes as date) as dtAlteracao
                                                                from bethadba.hist_salariais hs
                                                                where hs.i_entidades = {entidade} and hs.i_funcionarios = {funcionario}
                                                                order by dt_alteracoes desc
                                                            """)

                        # caso não retorne nenhum histórico, vai retornar um erro no log
                        if len(lastHistSalarial) == 0:
                            print(colorama.Fore.RED, f"Não localizado nenhuma variável posterior a data {dtInicial} e nenhum historico salarial para o funcionário {funcionario} da entidade {entidade}, favor analisar manualmente o caso.", colorama.Fore.RESET)
                            continue
                        else:
                            lastHistSalarial = lastHistSalarial[0]
                            newDtFinalLancamento = lastHistSalarial['dtAlteracao']
                            newDtFinalLancamento -= datetime.timedelta(days=1)
                            newDtFinalLancamento = datetime.date(newDtFinalLancamento.year, newDtFinalLancamento.month, 1)

                    else:
                        nextLancamento = nextLancamento[0]
                        newDtFinalLancamento = nextLancamento['dt_inicial']
                        newDtFinalLancamento -= datetime.timedelta(days=1)
                        newDtFinalLancamento = datetime.date(newDtFinalLancamento.year, newDtFinalLancamento.month, 1)

                    dadoAlterado.append(f"Alterado a data final para {newDtFinalLancamento} do lançamento com data inicial {dtInicial} para o evento {evento} do funcionário {funcionario} da entidade {entidade}")
                    comandoUpdate += f"""UPDATE Folharh.bethadba.variaveis SET dt_final = '{newDtFinalLancamento}' 
                                            WHERE dt_final = '{dtFinal}' and i_entidades = {entidade} and i_funcionarios = {funcionario} and i_eventos = {evento} and i_tipos_proc = {tipoProc} and i_processamentos = {processamento};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_lancamento_faltante: {e}")
            return

        if lancamento_faltante:
            dado = analisa_lancamento_faltante()

            if corrigirErros and len(dado) > 0:
                corrige_lancamento_faltante(listDados=dado)

    def analisa_corrige_lancamento_posterior_rescisao(pre_validacao):
        nomeValidacao = "Lançamento posterior a data da rescisão."

        def analisa_lancamento_posterior_rescisao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_lancamento_posterior_rescisao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoDelete = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            SELECT 
                                                hf.i_entidades,
                                                hf.i_funcionarios,
                                                r.dt_rescisao,
                                                v2.i_eventos,
                                                v2.i_tipos_proc,
                                                v2.i_processamentos,
                                                v2.dt_inicial,
                                                v2.dt_final,
                                                DATEDIFF(day, v2.dt_inicial, v2.dt_final) as dif
                                            FROM bethadba.hist_funcionarios hf
                                            INNER JOIN bethadba.hist_cargos hc ON hf.i_entidades = hc.i_entidades and hf.i_funcionarios = hc.i_funcionarios and hf.dt_alteracoes <= hc.dt_alteracoes 
                                            INNER JOIN bethadba.funcionarios f ON f.i_funcionarios = hf.i_funcionarios AND f.i_entidades = hf.i_entidades
                                            INNER JOIN bethadba.rescisoes r ON r.i_funcionarios = hf.i_funcionarios and r.i_entidades = hf.i_entidades
                                            INNER JOIN bethadba.variaveis v2 ON r.i_entidades = v2.i_entidades and r.i_funcionarios = v2.i_funcionarios 
                                            INNER JOIN bethadba.vinculos v ON v.i_vinculos = hf.i_vinculos
                                            WHERE (r.dt_rescisao < v2.dt_inicial OR r.dt_rescisao < v2.dt_final)  AND
                                                r.i_motivos_apos is null AND
                                                r.dt_canc_resc is null AND
                                                r.i_rescisoes  = (select max(r.i_rescisoes)
                                                                    from bethadba.rescisoes r
                                                                    where  r.i_entidades = hf.i_entidades and 
                                                                          r.i_funcionarios = hf.i_funcionarios  and r.dt_canc_resc is null and r.i_motivos_apos is null)
                                            GROUP BY hf.i_entidades, hf.i_funcionarios, v2.i_eventos, v2.i_tipos_proc, v2.i_processamentos, r.dt_rescisao, v2.dt_inicial, v2.dt_final 
                                            ORDER BY hf.i_entidades, hf.i_funcionarios, v2.i_eventos
                                        """)

                for row in dados:
                    # deletando as variáveis, pois as datas são chaves e não é possivel alterar
                    comandoDelete += f"""DELETE from bethadba.variaveis 
                                        where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_eventos = {row['i_eventos']} and i_tipos_proc = {row['i_tipos_proc']} and
                                         i_processamentos = {row['i_processamentos']} and dt_inicial = '{row['dt_inicial']}' and dt_final = '{row['dt_final']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoDelete, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_lancamento_posterior_rescisao: {e}")
            return

        if lancamento_posterior_rescisao:
            dado = analisa_lancamento_posterior_rescisao()

            if corrigirErros and len(dado) > 0:
                corrige_lancamento_posterior_rescisao(listDados=dado)

    def analisa_corrige_lancamento_posterior_cessacao(pre_validacao):
        nomeValidacao = "Lançamento posterior a data da cessação."

        def analisa_lancamento_posterior_cessacao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_lancamento_posterior_cessacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoDelete = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            SELECT 
                                                hf.i_entidades,
                                                hf.i_funcionarios,
                                                r.dt_rescisao,
                                                v2.i_eventos,
                                                v2.i_tipos_proc,
                                                v2.i_processamentos,
                                                v2.dt_inicial,
                                                v2.dt_final,
                                                DATEDIFF(day, v2.dt_inicial, v2.dt_final) as dif
                                            FROM bethadba.hist_funcionarios hf
                                            INNER JOIN bethadba.hist_cargos hc ON hf.i_entidades = hc.i_entidades and hf.i_funcionarios = hc.i_funcionarios and hf.dt_alteracoes <= hc.dt_alteracoes 
                                            INNER JOIN bethadba.funcionarios f ON f.i_funcionarios = hf.i_funcionarios AND f.i_entidades = hf.i_entidades
                                            INNER JOIN bethadba.rescisoes r ON r.i_funcionarios = hf.i_funcionarios and r.i_entidades = hf.i_entidades
                                            INNER JOIN bethadba.variaveis v2 ON r.i_entidades = v2.i_entidades and r.i_funcionarios = v2.i_funcionarios 
                                            INNER JOIN bethadba.vinculos v ON v.i_vinculos = hf.i_vinculos
                                            WHERE (r.dt_rescisao < v2.dt_inicial OR r.dt_rescisao < v2.dt_final)  AND
                                                r.i_motivos_apos is null AND
                                                r.dt_canc_resc is null AND
                                                r.i_rescisoes  = (select max(r.i_rescisoes)
                                                                    from bethadba.rescisoes r
                                                                    where  r.i_entidades = hf.i_entidades and 
                                                                          r.i_funcionarios = hf.i_funcionarios  and r.dt_canc_resc is null and r.i_motivos_apos is null)
                                            GROUP BY hf.i_entidades, hf.i_funcionarios, v2.i_eventos, v2.i_tipos_proc, v2.i_processamentos, r.dt_rescisao, v2.dt_inicial, v2.dt_final 
                                            ORDER BY hf.i_entidades, hf.i_funcionarios, v2.i_eventos
                                        """)

                for row in dados:
                    # deletando as variáveis, pois as datas são chaves e não é possivel alterar
                    comandoDelete += f"""DELETE from bethadba.variaveis 
                                        where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_eventos = {row['i_eventos']} and i_tipos_proc = {row['i_tipos_proc']} and
                                         i_processamentos = {row['i_processamentos']} and dt_inicial = '{row['dt_inicial']}' and dt_final = '{row['dt_final']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoDelete, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_lancamento_posterior_cessacao: {e}")
            return

        if lancamento_posterior_cessacao:
            dado = analisa_lancamento_posterior_cessacao()

            if corrigirErros and len(dado) > 0:
                corrige_lancamento_posterior_cessacao(listDados=dado)

    if dadosList:
        analisa_corrige_lancamento_faltante(pre_validacao="lancamento_faltante")
        analisa_corrige_lancamento_posterior_rescisao(pre_validacao="lancamento_posterior_rescisao")
        analisa_corrige_lancamento_posterior_cessacao(pre_validacao="lancamento_posterior_cessacao")
