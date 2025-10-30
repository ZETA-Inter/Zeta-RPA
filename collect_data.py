# Importações
import psycopg2
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import re
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException

# -=-=-=-=-=-=-=-=-=-=-=-=-= FUNÇÕES -=-=-=-=-=-=-=-=-=-=-=-=-=
# Atualiza a tabela plans
def update_plans(cur, conn):
    try:
        # Query para buscar as informações no staging
        query_subscription = """ 
        SELECT 
            id, 
            tp_plano, 
            preco_fixo
        FROM staging.assinatura;
        """

        # Query para buscar as informações na tabela plans do segundo
        query_plans = "SELECT * FROM plans;"

        # Executa no schema staging
        cur.execute(query_subscription)
        subscriptions = cur.fetchall()

        # Executa na tabela plans
        cur.execute(query_plans)
        plans = cur.fetchall()

        # Deleta os registros que não estão na tabela de origem
        valid_ids = [sub[0] for sub in subscriptions]
        if valid_ids:
            delete_query = "DELETE FROM plans WHERE id NOT IN %s;"
            cur.execute(delete_query, (tuple(valid_ids),))
            conn.commit()
        else:
            print("Nenhuma deleção realizada.")

        # Atualiza ou insere os registros na tabela 'plans'
        for id_subscription, subscription, price in subscriptions:
            price_clean = float(price.replace("$", "").replace(",", "").strip()) # tirar o "$" do valor do preço da tabela de origem

            exist_ids = [plan[0] for plan in plans] # pega os ids da tabela 'plans'

            if id_subscription in exist_ids:
                update_query = "UPDATE plans SET name = %s, value = %s WHERE id = %s;"
                cur.execute(update_query, (subscription, price_clean, id_subscription))
            
            else:
                insert_query = "INSERT INTO plans (id, name, value) VALUES (%s, %s, %s);"
                cur.execute(insert_query, (id_subscription, subscription, price_clean))
                
            conn.commit()
        print("Tabela 'plans' atualizada com sucesso!")
    except Exception as e:
        print(f"Erro ao atualizar a tabela 'plans': {e}")

# Atualiza a tabela segments
def update_segments(cur, conn):
    try:
        # Query para buscar as informações no staging
        query_course = """ 
        SELECT 
            id, 
            nome
        FROM staging.curso;
        """

        # Query para buscar as informações na tabela segments do segundo
        query_segments = "SELECT * FROM segments;"

        # Executa no schema staging
        cur.execute(query_course)
        segments = cur.fetchall()

        # Executa na tabela segments
        cur.execute(query_segments)
        existing_segments = cur.fetchall()

        # Deleta os registros que não estão na tabela de origem
        valid_ids = [seg[0] for seg in segments]
        if valid_ids:
            delete_query = "DELETE FROM segments WHERE id NOT IN %s;"
            cur.execute(delete_query, (tuple(valid_ids),))
            conn.commit()
        else:
            print("Nenhuma deleção realizada.")

        # Atualiza ou insere os registros na tabela 'segments'
        for id_segment, segment_name in segments:

            exist_ids = [seg[0] for seg in existing_segments] # pega os ids da tabela 'segments'

            if id_segment in exist_ids:
                update_query = "UPDATE segments SET name = %s WHERE id = %s;"
                cur.execute(update_query, (segment_name, id_segment))
            
            else:
                insert_query = "INSERT INTO segments (id, name) VALUES (%s, %s);"
                cur.execute(insert_query, (id_segment, segment_name))
                
            conn.commit()
        print("Tabela 'segments' atualizada com sucesso!")
    except Exception as e:
        print(f"Erro ao atualizar a tabela 'segments': {e}")

# Atualiza a tabela workers
def update_workers(cur, conn):
    try:
        # Query para buscar as informações no staging
        query_worker = """ 
        SELECT
	        p.id,
	        p.email,
	        p.nome_primeiro||' '||p.nome_ultimo AS nome_completo,
	        f.id_empresa AS company_id
        FROM staging.produtor p
        JOIN staging.fornecedor f ON p.id_fornecedor = f.id;
        """

        # Query para buscar as informações na tabela workers do segundo
        query_workers = "SELECT id, email, name, company_id FROM workers;"

        # Executa no schema staging
        cur.execute(query_worker)
        workers = cur.fetchall()

        # Executa na tabela workers
        cur.execute(query_workers)
        existing_workers = cur.fetchall()

        # Deleta os registros que não estão na tabela de origem
        valid_ids = [w[0] for w in workers]
        if valid_ids:
            delete_query = "UPDATE workers SET active = false WHERE id NOT IN %s;"
            cur.execute(delete_query, (tuple(valid_ids),))
            conn.commit()
        else:
            print("Nenhuma usuário foi desativado.")

        # Atualiza ou insere os registros na tabela 'workers'
        for id_worker, worker_email, worker_name, company_id in workers:

            exist_ids = [w[0] for w in existing_workers] # pega os ids da tabela 'workers'

            if id_worker in exist_ids:
                update_query = "UPDATE workers SET name = %s, email = %s, company_id = %s WHERE id = %s;"
                cur.execute(update_query, (worker_name, worker_email, company_id, id_worker))
            
            else:
                insert_query = "INSERT INTO workers (id, email, name, company_id, created_at, active) VALUES (%s, %s, %s, %s, (SELECT CURRENT_DATE), true);"
                cur.execute(insert_query, (id_worker, worker_email, worker_name, company_id))
                
            conn.commit()
        print("Tabela 'workers' atualizada com sucesso!")
    except Exception as e:
        print(f"Erro ao atualizar tabela 'workers': {e}")
    
# Atualiza a collection activities no MongoDB
def update_activities(coll_activities, cur):
    try:
        # Busca o id da atividade, pontuação, id da aula, perguntas e as alternativas no postgres
        query = """
        SELECT 
            a.id AS atividade_id,
            a.pontuacao,
            a.id_aula AS class_id,
            p.id AS pergunta_id,
            p.pergunta,
            alt.id AS alternativa_id,
            alt.alternativa,
            alt.correta
        FROM staging.atividade a
        LEFT JOIN staging.pergunta p ON p.id_atividade = a.id
        LEFT JOIN staging.alternativa alt ON alt.id_atividade = a.id
        ORDER BY a.id, p.id, alt.id;
        """
        cur.execute(query)
        rows = cur.fetchall()

        # Monta a estrutura agrupada
        activities = {}
        for r in rows:
            activity_id, points, class_id, question_id, question, alternative_id, alternative, correct = r

            # Se o id da atividade não estiver no dicionário
            if activity_id not in activities:
                activities[activity_id] = {
                    "_id": activity_id,
                    "class_id": class_id,
                   "points": float(points) if points is not None else 0,
                    "questions": {}
                }
            
            # Pula perguntas sme id
            if not question_id:
                continue

            # Se as perguntas e seus ids não estiverem no dicionário
            if question_id not in activities[activity_id]["questions"]:
                activities[activity_id]["questions"][question_id] = {
                    "question": question,
                    "answers": []
                }

            # adiciona alternativas
            if alternative:
                activities[activity_id]["questions"][question_id]["answers"].append({
                    "answer": alternative,
                    "correct": correct
                })

        # Converte questions de dict para lista
        for activity in activities.values():
            activity["questions"] = list(activity["questions"].values())

        # Atualiza o mongo
        existing_docs = list(coll_activities.find({}, {"_id": 1}))
        existing_ids = {doc["_id"] for doc in existing_docs}
        valid_ids = set(activities.keys())

        # Insere se não existir e atualiza se for necessário
        inserted = 0
        updated = 0
        for atividade_id, data in activities.items():
            try:
                existing = coll_activities.find_one({"_id": atividade_id})
                if not existing: # se não existir, ele insere
                    coll_activities.insert_one(data)
                    inserted += 1
                    print(f"Atividade {atividade_id} inserida.")
                else: # verificar se precisa atualizar
                    if existing != data:
                        coll_activities.replace_one({"_id": atividade_id}, data)
                        updated += 1
                        print(f"Atividade {atividade_id} atualizada.")
            except Exception as e:
                print(f"Erro ao processar atividade {atividade_id}: {e}")

        # Deleta os documentos que não estão mais no postgres
        to_delete = existing_ids - valid_ids
        deleted = 0
        if to_delete:
            result = coll_activities.delete_many({"_id": {"$in": list(to_delete)}})
            deleted = result.deleted_count
            print(f"Total de atividades deletadas: {deleted}")

        # Resumo geral
        print("Collection activities sincronizada com sucesso!")
        print(f"Inseridos: {inserted}")
        print(f"Atualizados: {updated}")
        print(f"Deletados: {deleted}")
    
    except Exception as e:
        print(f"Erro ao sincronizar atividades: {e}")

# Ajuda ao dividir o texto text_corrido da tabela texto_corrido em partes menores para o campo content (na collection classes)
def split_text(text, max_len=250):
            sentences = re.split(r'(?<=[.,])\s+', text)
            parts = []
            current = ""

            for sentence in sentences:
                if len(current) + len(sentence) + 1 > max_len:
                    parts.append(current.strip())
                    current = sentence
                else:
                    current += " " + sentence

            if current:
                parts.append(current.strip())
            return parts

# Com base na lei recebida do banco, pesquisa no site da defesa agropecuária de São Paulo
def search_law(law_number: str):
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")

        # Conecta ao site da Câmara (para buscar as leis)
        driver = webdriver.Chrome(options=chrome_options)
        driver.get("https://www.defesa.agricultura.sp.gov.br/legislacoes")
        time.sleep(2)

        # Verifica cada categoria de leis
        type_id = None
        if "decreto" in law_number.lower():
            type_id = "id_tipo_leg[]_1"
        elif "lei complementar" in law_number.lower():
            type_id = "id_tipo_leg[]_4"
        elif "lei" in law_number.lower():
            type_id = "id_tipo_leg[]_5"
        elif "instrução normativa" in law_number.lower() or "in " in law_number.lower():
            type_id = "id_tipo_leg[]_2"
        elif "portaria" in law_number.lower():
            type_id = "id_tipo_leg[]_6"
        elif "nota técnica" in law_number.lower():
            type_id = "id_tipo_leg[]_8"
        elif "resolução" in law_number.lower():
            type_id = "id_tipo_leg[]_7"

        # caso type_id esteja vazio
        if not type_id:
            return None

        try:
            # Se não estiver vazio, clica na opção correspondente da página
            driver.find_element(By.ID, type_id).click()
        except NoSuchElementException:
            return None
        
        # Expressão regular para pegar número e ano
        match = re.search(r'([\d\.]+)\s*/\s*(\d{4})', law_number)
        if match:
            law_number = match.group(1)
            law_year = match.group(2)
        else:
            law_number, law_year = None, None
            return None
        
        time.sleep(2)

        # Preenche o número da lei
        try:
            number_box = driver.find_element(By.XPATH, "/html/body/main/section[2]/div[2]/table/tbody/tr[1]/td/form/table[2]/tbody/tr[1]/td[2]/input")
            number_box.send_keys(law_number)
            time.sleep(2)

        except Exception as e:
            return None

        # Clica no botão de buscar
        try:
            driver.find_element(By.XPATH, '/html/body/main/section[2]/div[2]/table/tbody/tr[1]/td/form/table[2]/tbody/tr[5]/td/input').click()
            time.sleep(2)
        except Exception as e:
            return None

        # Scrolla a tela para baixo
        try:
            driver.execute_script("window.scrollTo(0, 200);")

            # Clica no primeiro resultado
            first_result = driver.find_element(By.XPATH, '/html/body/main/section[2]/div[2]/ul[1]/li/p/a')
            if first_result:
                first_result.click()
            else: 
                return None
            time.sleep(2)

            # Pega o texto da ementa
            law_description = driver.find_element(By.XPATH, '/html/body/main/section[2]/div[2]/p[2]').text
            return law_description
            
        except Exception as e:
            return None

    except (TimeoutException, Exception) as e:
        print("Ocorreu um erro durante a execução do script:", e)
        return None

# Atualiza collection classes no MongoDB
def update_classes(coll_classes, cur):
    try:
        # Query para buscar as informações no staging
        query = """ 
        SELECT
            a.id AS aula_id,
            a.nome AS aula_nome,
            a.id_modulo AS modulo_id,
            m.modulo AS modulo_nome,
            t.texto_corrido,
            f.frente AS flashcard_frente,
            f.verso AS flashcard_verso,
            l.lei AS lei_numero
        FROM staging.aula a
        LEFT JOIN staging.modulo m ON a.id_modulo = m.id
        LEFT JOIN staging.texto_corrido t ON t.id_aula = a.id
        LEFT JOIN staging.flash_card f ON f.id_aula = a.id
        LEFT JOIN staging.lei l ON l.id_aula = a.id
        ORDER BY a.id;
        """

        # Executa no schema staging
        cur.execute(query)
        rows = cur.fetchall()

        # Monta a estrutura agrupada por aula
        classes = {}
        for r in rows:
            class_id, class_name, module_id, module_name, text, front, back, law_number = r

            if class_id not in classes:
                classes[class_id] = {
                    "_id": class_id,
                    "program_id": module_id,
                    "title": class_name,
                    "description": module_name,
                    "content": [],
                    "flashcards": [],
                    "laws": [],
                    # sets de controle (evitam duplicações)
                    "_seen_content_parts": set(),
                    "_seen_flashcards": set(),
                    "_seen_laws": set()
                }

            doc = classes[class_id]

            # adiciona texto corrido (quebra em partes) - sem duplicar
            if text:
                parts = split_text(text)
                for part in parts:
                    if part not in doc["_seen_content_parts"]:
                        doc["_seen_content_parts"].add(part)
                        doc["content"].append(part)

            # adiciona flashcards - sem duplicar
            if front and back:
                key = f"{front}||{back}"
                if key not in doc["_seen_flashcards"]:
                    doc["_seen_flashcards"].add(key)
                    doc["flashcards"].append({
                        "front": front,
                        "back": back
                    })

             # adiciona leis (a descrição será preenchida por outro RPA futuramente) - sem duplicar
            if law_number and law_number not in doc["_seen_laws"]:
                law_description = search_law(law_number) # Chama a função search_law (um web scraping para a lei)
                doc["_seen_laws"].add(law_number)
                doc["laws"].append({
                    "number": law_number,
                    "description": law_description
                })

        # Remove os sets de controle antes de enviar ao MongoDB
        for c in classes.values():
            c.pop("_seen_content_parts", None)
            c.pop("_seen_flashcards", None)
            c.pop("_seen_laws", None)

        # Atualiza o MongoDB
        existing_docs = list(coll_classes.find({}, {"_id": 1}))
        existing_ids = {doc["_id"] for doc in existing_docs}
        valid_ids = set(classes.keys())

        inserted = 0
        updated = 0

        for aula_id, data in classes.items():
            existing = coll_classes.find_one({"_id": aula_id})
            if not existing:
                coll_classes.insert_one(data)
                inserted += 1
            else:
                # se houver diferença, atualiza
                if existing != data:
                    coll_classes.replace_one({"_id": aula_id}, data)
                    updated += 1

        # Remove aulas que não existem mais no Postgres
        to_delete = existing_ids - valid_ids
        deleted = 0
        if to_delete:
            result = coll_classes.delete_many({"_id": {"$in": list(to_delete)}})
            deleted = result.deleted_count

        print(f"Sincronização concluída com sucesso!")
        print(f"Inseridos: {inserted}")
        print(f"Atualizados: {updated}")
        print(f"Deletados: {deleted}")
    except Exception as e:
        print(f"Erro ao sincronizar a collection 'classes': {e}")

# Chamando as funções 
if "__main__" == __name__:
    try:
        # Declarando as variáveis de ambiente
        load_dotenv()

        # banco Postgres
        POSTGRES_URL = os.getenv("POSTGRES_URL_2")
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor()

        # banco MongoDB
        mongo_url = os.getenv("MONGODB_URL")
        client = MongoClient(mongo_url)
        dbZeta = client["Zeta"] 
        activities = dbZeta['activities']
        classes = dbZeta['classes']

        # Atualizando a tabela plans
        print("Atualizando a tabela 'plans'...")
        update_plans(cur, conn)

        # Atualizando a tabela segments
        print("\nAtualizando a tabela 'segments'...")
        update_segments(cur, conn)

        # Atualiza a tabela workers 
        print("\nAtualizando a tabela 'workers'...")
        update_workers(cur, conn)

        # Atualiza ou insere documentos na collection activities
        print("\nSincronizando a collection 'activities'...")
        update_activities(activities, cur)

        # Atuliza ou insere documentos na collection classes
        print("\nAtualizando a collection 'classes'...")
        update_classes(classes, cur)
    except Exception as e:
        print(f"Erro ao executar o RPA: {e}")
    finally:
        # Fecha as conexões
        cur.close()
        conn.close()
        client.close()