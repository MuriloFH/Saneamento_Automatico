from utilitarios.conexao.conectaOdbc import Conecta


def buscaDados(tipo_registro, odbc, logSistema):
    banco = Conecta(odbc=odbc)
    print(f"> Iniciando a coleta do registro {tipo_registro}")
    logSistema.escreveLog(f"> Iniciando a coleta do registro {tipo_registro}")
    resultado = banco.consultar(comando=f"""SELECT
                                                trim(reg.tipo_registro) as tipo_registro,
                                                ocor.pre_validacao,
                                                ocor.mensagem_erro,
                                                ocor.situacao,
                                                i_chave_dsk1, i_chave_dsk2, i_chave_dsk3, i_chave_dsk4, i_chave_dsk5, i_chave_dsk6, i_chave_dsk7, i_chave_dsk8, i_chave_dsk9, i_chave_dsk10, i_chave_dsk11, i_chave_dsk12
                                            FROM
                                                bethadba.controle_migracao_registro reg// HOLDLOCK
                                                JOIN bethadba.controle_migracao_registro_ocor ocor
                                            WHERE
                                                reg.tipo_registro = '{tipo_registro}'
                                                and ocor.situacao != 2
                                            ORDER BY ocor.tipo_registro, ocor.pre_validacao
                                """)

    if len(resultado) == 0:
        print(f"> Nenhum registro: {tipo_registro} localizado!")
        logSistema.escreveLog(f"> Nenhum registro: {tipo_registro} localizado!")
        return False
    else:
        print(f"> Total de registro do tipo {tipo_registro} coletados: {len(resultado)}.")
        logSistema.escreveLog(f"> Total de registro do tipo {tipo_registro} coletados: {len(resultado)}.")
        return resultado
