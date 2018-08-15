--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.3
-- Dumped by pg_dump version 10.4

-- Started on 2018-08-15 12:56:29 AWST

\connect vospace

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

CREATE EXTENSION IF NOT EXISTS ltree WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;

--
-- TOC entry 287 (class 1255 OID 57417)
-- Name: delete_notify_trigger(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.delete_notify_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$

DECLARE

BEGIN
PERFORM
pg_notify(TG_TABLE_NAME, '{"action":"' || TG_OP || '","table":"' || TG_TABLE_NAME || '","row":' || row_to_json(OLD) || '}');
RETURN OLD;
END;
$$;


ALTER FUNCTION public.delete_notify_trigger() OWNER TO "vos_user";

--
-- TOC entry 288 (class 1255 OID 57416)
-- Name: insert_notify_trigger(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.insert_notify_trigger() RETURNS trigger
    LANGUAGE plpgsql
    AS $$DECLARE

BEGIN
IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.phase <> OLD.phase) THEN
PERFORM
pg_notify(TG_TABLE_NAME, '{"action":"' || TG_OP || '","table":"' || TG_TABLE_NAME || '","row":' || row_to_json(NEW) || '}');
RETURN NEW;
END IF;
RETURN NULL;
END;

$$;


ALTER FUNCTION public.insert_notify_trigger() OWNER TO "vos_user";

--
-- TOC entry 289 (class 1255 OID 57415)
-- Name: update_modified_column(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.update_modified_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.modified = now();
    RETURN NEW;   
END;
$$;


ALTER FUNCTION public.update_modified_column() OWNER TO "vos_user";

--
-- TOC entry 290 (class 1255 OID 57533)
-- Name: update_path_modified_column(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.update_path_modified_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
   IF NEW.path <> OLD.path THEN
   NEW.path_modified := OLD.path_modified + 1;
   END IF;
   RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_path_modified_column() OWNER TO "vos_user";

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 187 (class 1259 OID 16620)
-- Name: nodes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.nodes (
    type smallint,
    name text,
    path public.ltree NOT NULL,
    busy boolean DEFAULT false NOT NULL,
    space_id bigint NOT NULL,
    link text,
    groupread text[] DEFAULT ARRAY[]::text[],
    groupwrite text[] DEFAULT ARRAY[]::text[],
    owner character varying(128) NOT NULL,
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    path_modified bigint DEFAULT 0 NOT NULL
);


ALTER TABLE public.nodes OWNER TO "vos_user";

--
-- TOC entry 188 (class 1259 OID 16634)
-- Name: properties; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.properties (
    uri text NOT NULL,
    value text,
    read_only boolean DEFAULT true,
    node_path public.ltree NOT NULL,
    space_id bigint NOT NULL
);


ALTER TABLE public.properties OWNER TO "vos_user";

--
-- TOC entry 190 (class 1259 OID 40980)
-- Name: space; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.space (
    host text NOT NULL,
    port smallint NOT NULL,
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    parameters jsonb
);


ALTER TABLE public.space OWNER TO "vos_user";

--
-- TOC entry 191 (class 1259 OID 40991)
-- Name: space_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.space_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.space_id_seq OWNER TO "vos_user";

--
-- TOC entry 2369 (class 0 OID 0)
-- Dependencies: 191
-- Name: space_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.space_id_seq OWNED BY public.space.id;


--
-- TOC entry 192 (class 1259 OID 49171)
-- Name: storage; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.storage (
    name character varying(128) NOT NULL,
    host text NOT NULL,
    port smallint NOT NULL,
    parameters jsonb NOT NULL,
    https boolean DEFAULT false NOT NULL,
    id bigint NOT NULL,
    online boolean DEFAULT false NOT NULL,
    enabled boolean DEFAULT false NOT NULL
);


ALTER TABLE public.storage OWNER TO "vos_user";

--
-- TOC entry 194 (class 1259 OID 57421)
-- Name: storage_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.storage_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.storage_id_seq OWNER TO "vos_user";

--
-- TOC entry 2372 (class 0 OID 0)
-- Dependencies: 194
-- Name: storage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.storage_id_seq OWNED BY public.storage.id;


--
-- TOC entry 193 (class 1259 OID 57351)
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    username text NOT NULL,
    groupread text[] DEFAULT ARRAY[]::text[],
    groupwrite text[] DEFAULT ARRAY[]::text[],
    password text NOT NULL,
    space_name text NOT NULL,
    admin boolean DEFAULT false NOT NULL
);


ALTER TABLE public.users OWNER TO "vos_user";

--
-- TOC entry 189 (class 1259 OID 24576)
-- Name: uws_jobs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.uws_jobs (
    phase integer NOT NULL,
    destruction timestamp without time zone NOT NULL,
    job_info xml NOT NULL,
    error text,
    transfer xml,
    space_id bigint NOT NULL,
    results xml,
    owner text NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    node_path_modified bigint,
    node_path public.ltree
);


ALTER TABLE public.uws_jobs OWNER TO "vos_user";

--
-- TOC entry 2194 (class 2604 OID 40993)
-- Name: space id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.space ALTER COLUMN id SET DEFAULT nextval('public.space_id_seq'::regclass);


--
-- TOC entry 2196 (class 2604 OID 57423)
-- Name: storage id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.storage ALTER COLUMN id SET DEFAULT nextval('public.storage_id_seq'::regclass);


--
-- TOC entry 2215 (class 2606 OID 57501)
-- Name: uws_jobs job_id_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.uws_jobs
    ADD CONSTRAINT job_id_pk PRIMARY KEY (id);


--
-- TOC entry 2204 (class 2606 OID 57538)
-- Name: nodes node_id_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT node_id_unique UNIQUE (id);


--
-- TOC entry 2206 (class 2606 OID 57536)
-- Name: nodes node_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT node_pk PRIMARY KEY (path, space_id);


--
-- TOC entry 2208 (class 2606 OID 41013)
-- Name: nodes nodes_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT nodes_unique UNIQUE (path, space_id);


--
-- TOC entry 2213 (class 2606 OID 41011)
-- Name: properties properties_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT properties_unique UNIQUE (uri, node_path, space_id);


--
-- TOC entry 2219 (class 2606 OID 41001)
-- Name: space space_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.space
    ADD CONSTRAINT space_pk PRIMARY KEY (id);


--
-- TOC entry 2221 (class 2606 OID 41003)
-- Name: space space_unique_host_port; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.space
    ADD CONSTRAINT space_unique_host_port UNIQUE (host, port);


--
-- TOC entry 2223 (class 2606 OID 49180)
-- Name: space space_unique_name; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.space
    ADD CONSTRAINT space_unique_name UNIQUE (name);


--
-- TOC entry 2226 (class 2606 OID 57433)
-- Name: storage storage_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.storage
    ADD CONSTRAINT storage_pk PRIMARY KEY (id);


--
-- TOC entry 2228 (class 2606 OID 57435)
-- Name: storage storage_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.storage
    ADD CONSTRAINT storage_unique UNIQUE (name, host, port);


--
-- TOC entry 2230 (class 2606 OID 57363)
-- Name: users user_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT user_pk PRIMARY KEY (space_name, username);


--
-- TOC entry 2202 (class 1259 OID 41009)
-- Name: fki_space_fk; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fki_space_fk ON public.nodes USING btree (space_id);


--
-- TOC entry 2224 (class 1259 OID 49186)
-- Name: fki_storage_fk; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fki_storage_fk ON public.storage USING btree (name);


--
-- TOC entry 2216 (class 1259 OID 57392)
-- Name: owner_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX owner_idx ON public.uws_jobs USING btree (owner bpchar_pattern_ops);


--
-- TOC entry 2209 (class 1259 OID 16630)
-- Name: path_gist_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX path_gist_idx ON public.nodes USING gist (path);


--
-- TOC entry 2210 (class 1259 OID 16631)
-- Name: path_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX path_idx ON public.nodes USING btree (path);


--
-- TOC entry 2217 (class 1259 OID 57540)
-- Name: phase_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX phase_idx ON public.uws_jobs USING btree (phase);


--
-- TOC entry 2211 (class 1259 OID 49159)
-- Name: properties_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX properties_idx ON public.properties USING btree (node_path);


--
-- TOC entry 2238 (class 2620 OID 57420)
-- Name: uws_jobs delete_trigger; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER delete_trigger AFTER DELETE ON public.uws_jobs FOR EACH ROW EXECUTE PROCEDURE public.delete_notify_trigger();


--
-- TOC entry 2236 (class 2620 OID 57418)
-- Name: uws_jobs insert_trigger; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER insert_trigger AFTER INSERT OR UPDATE ON public.uws_jobs FOR EACH ROW EXECUTE PROCEDURE public.insert_notify_trigger();


--
-- TOC entry 2235 (class 2620 OID 57539)
-- Name: nodes path_change_trigger; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER path_change_trigger BEFORE UPDATE ON public.nodes FOR EACH ROW EXECUTE PROCEDURE public.update_path_modified_column();


--
-- TOC entry 2237 (class 2620 OID 57419)
-- Name: uws_jobs update_trigger; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER update_trigger AFTER UPDATE ON public.uws_jobs FOR EACH ROW EXECUTE PROCEDURE public.update_modified_column();


--
-- TOC entry 2232 (class 2606 OID 49154)
-- Name: properties properties_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT properties_fk FOREIGN KEY (node_path, space_id) REFERENCES public.nodes(path, space_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2231 (class 2606 OID 41004)
-- Name: nodes space_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT space_fk FOREIGN KEY (space_id) REFERENCES public.space(id) ON UPDATE SET NULL ON DELETE SET NULL;


--
-- TOC entry 2233 (class 2606 OID 49160)
-- Name: uws_jobs space_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.uws_jobs
    ADD CONSTRAINT space_fk FOREIGN KEY (space_id) REFERENCES public.space(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2234 (class 2606 OID 57436)
-- Name: storage storage_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.storage
    ADD CONSTRAINT storage_fk FOREIGN KEY (name) REFERENCES public.space(name) ON UPDATE CASCADE;


--
-- TOC entry 2362 (class 0 OID 0)
-- Dependencies: 287
-- Name: FUNCTION delete_notify_trigger(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.delete_notify_trigger() TO "vos_user";


--
-- TOC entry 2363 (class 0 OID 0)
-- Dependencies: 288
-- Name: FUNCTION insert_notify_trigger(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.insert_notify_trigger() TO "vos_user";


--
-- TOC entry 2364 (class 0 OID 0)
-- Dependencies: 289
-- Name: FUNCTION update_modified_column(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.update_modified_column() TO "vos_user";


--
-- TOC entry 2365 (class 0 OID 0)
-- Dependencies: 290
-- Name: FUNCTION update_path_modified_column(); Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON FUNCTION public.update_path_modified_column() TO "vos_user";


--
-- TOC entry 2366 (class 0 OID 0)
-- Dependencies: 187
-- Name: TABLE nodes; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,TRUNCATE,UPDATE ON TABLE public.nodes TO "vos_user";


--
-- TOC entry 2367 (class 0 OID 0)
-- Dependencies: 188
-- Name: TABLE properties; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,TRIGGER,UPDATE ON TABLE public.properties TO "vos_user";


--
-- TOC entry 2368 (class 0 OID 0)
-- Dependencies: 190
-- Name: TABLE space; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.space TO "vos_user";


--
-- TOC entry 2370 (class 0 OID 0)
-- Dependencies: 191
-- Name: SEQUENCE space_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.space_id_seq TO "vos_user";


--
-- TOC entry 2371 (class 0 OID 0)
-- Dependencies: 192
-- Name: TABLE storage; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.storage TO "vos_user";


--
-- TOC entry 2373 (class 0 OID 0)
-- Dependencies: 194
-- Name: SEQUENCE storage_id_seq; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON SEQUENCE public.storage_id_seq TO "vos_user";


--
-- TOC entry 2374 (class 0 OID 0)
-- Dependencies: 193
-- Name: TABLE users; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.users TO "vos_user";


--
-- TOC entry 2375 (class 0 OID 0)
-- Dependencies: 189
-- Name: TABLE uws_jobs; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,TRIGGER,UPDATE ON TABLE public.uws_jobs TO "vos_user";


-- Completed on 2018-08-15 12:56:33 AWST

--
-- PostgreSQL database dump complete
--

