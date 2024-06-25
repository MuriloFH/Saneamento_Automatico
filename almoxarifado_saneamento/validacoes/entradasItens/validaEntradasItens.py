from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def entradasItens(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                   corrigirErros=False,
                   codigo_material_duplicado=False
                   ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    # dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_codigo_material_duplicado(pre_validacao):
        nomeValidacao = "Entradas Itens com material em duplicidade"

        def analisa_codigo_material_duplicado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = banco.consultar("""
                select
                    i_entrada,
                    count(*),
                    i_material,
                    sum(quantidade) as quantidade,
                    sum(vlrtotal) as vlrtotal,
                    max(item) as maior_item,
                    max(i_classe)  as i_classe,
                    i_entidades 
                from bethadba.movimentos where i_entrada is not null
                group by i_entrada,i_material,i_entidades
                having count(*) > 1
                order by 1
            """)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_codigo_material_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            comandoDelete = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""
                    select
                        i_entrada,
                        count(*),
                        i_material,
                        sum(quantidade) as quantidade,
                        sum(vlrtotal) as vlrtotal,
                        max(item) as maior_item,
                        max(i_classe) as i_classe,
                        i_entidades 
                    from bethadba.movimentos where i_entrada is not null
                    group by i_entrada,i_material,i_entidades
                    having count(*) > 1
                    order by 1  
                """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Agrupado o material {row['i_material']} da entrada {row['i_entrada']}")
                        # delete
                        comandoDelete += f"delete bethadba.movimentos where i_entrada = {row['i_entrada']} and i_material = {row['i_material']} and i_entidades = {row['i_entidades']};\n"

                        # insert
                        comandoInsert += f"insert into bethadba.movimentos(i_entrada,i_classe,i_material,quantidade,vlrtotal,estornado,item,i_entidades) values({row['i_entrada']},{row['i_classe']},{row['i_material']},{row['quantidade']},{row['vlrtotal']},'N',{row['maior_item']},{row['i_entidades']});\n"

                # delete
                banco.executar(comando=banco.triggerOff(comandoDelete))

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)


                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_codigo_material_duplicado: {e}")
            return

        if codigo_material_duplicado:
            dado = analisa_codigo_material_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_codigo_material_duplicado(listDados=dado)

    # Não possui pré validação no Arqjob
    analisa_corrige_codigo_material_duplicado(pre_validacao="codigo_material_duplicado")





