--
-- PostgreSQL database dump
--

-- Dumped from database version 10.5 (Debian 10.5-1.pgdg90+1)
-- Dumped by pg_dump version 10.4

-- Started on 2018-09-07 14:03:44 AWST

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

--
-- TOC entry 1 (class 3079 OID 12980)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- TOC entry 3088 (class 0 OID 0)
-- Dependencies: 1
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- TOC entry 3 (class 3079 OID 16386)
-- Name: ltree; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS ltree WITH SCHEMA public;


--
-- TOC entry 3089 (class 0 OID 0)
-- Dependencies: 3
-- Name: EXTENSION ltree; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION ltree IS 'data type for hierarchical tree-like structures';


--
-- TOC entry 2 (class 3079 OID 16561)
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- TOC entry 3090 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- TOC entry 298 (class 1255 OID 16572)
-- Name: delete_notify_trigger(); Type: FUNCTION; Schema: public; Owner: vos_user
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


ALTER FUNCTION public.delete_notify_trigger() OWNER TO vos_user;

--
-- TOC entry 299 (class 1255 OID 16573)
-- Name: insert_notify_trigger(); Type: FUNCTION; Schema: public; Owner: vos_user
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


ALTER FUNCTION public.insert_notify_trigger() OWNER TO vos_user;

--
-- TOC entry 300 (class 1255 OID 16574)
-- Name: update_modified_column(); Type: FUNCTION; Schema: public; Owner: vos_user
--

CREATE FUNCTION public.update_modified_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.modified = now();
    RETURN NEW;   
END;
$$;


ALTER FUNCTION public.update_modified_column() OWNER TO vos_user;

--
-- TOC entry 301 (class 1255 OID 16575)
-- Name: update_path_modified_column(); Type: FUNCTION; Schema: public; Owner: vos_user
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


ALTER FUNCTION public.update_path_modified_column() OWNER TO vos_user;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 198 (class 1259 OID 16576)
-- Name: nodes; Type: TABLE; Schema: public; Owner: vos_user
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
    path_modified bigint DEFAULT 0 NOT NULL,
    storage_id bigint,
    size bigint DEFAULT 0 NOT NULL
);


ALTER TABLE public.nodes OWNER TO vos_user;

--
-- TOC entry 199 (class 1259 OID 16587)
-- Name: properties; Type: TABLE; Schema: public; Owner: vos_user
--

CREATE TABLE public.properties (
    uri text NOT NULL,
    value text,
    read_only boolean DEFAULT true,
    node_path public.ltree NOT NULL,
    space_id bigint NOT NULL
);


ALTER TABLE public.properties OWNER TO vos_user;

--
-- TOC entry 200 (class 1259 OID 16594)
-- Name: space; Type: TABLE; Schema: public; Owner: vos_user
--

CREATE TABLE public.space (
    host text NOT NULL,
    port smallint NOT NULL,
    id bigint NOT NULL,
    name character varying(128) NOT NULL,
    parameters jsonb
);


ALTER TABLE public.space OWNER TO vos_user;

--
-- TOC entry 201 (class 1259 OID 16600)
-- Name: space_id_seq; Type: SEQUENCE; Schema: public; Owner: vos_user
--

CREATE SEQUENCE public.space_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.space_id_seq OWNER TO vos_user;

--
-- TOC entry 3091 (class 0 OID 0)
-- Dependencies: 201
-- Name: space_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: vos_user
--

ALTER SEQUENCE public.space_id_seq OWNED BY public.space.id;


--
-- TOC entry 202 (class 1259 OID 16602)
-- Name: storage; Type: TABLE; Schema: public; Owner: vos_user
--

CREATE TABLE public.storage (
    name character varying(128) NOT NULL,
    host text NOT NULL,
    port smallint NOT NULL,
    parameters jsonb NOT NULL,
    https boolean DEFAULT false NOT NULL,
    id bigint NOT NULL,
    enabled boolean DEFAULT false NOT NULL
);


ALTER TABLE public.storage OWNER TO vos_user;

--
-- TOC entry 203 (class 1259 OID 16611)
-- Name: storage_id_seq; Type: SEQUENCE; Schema: public; Owner: vos_user
--

CREATE SEQUENCE public.storage_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.storage_id_seq OWNER TO vos_user;

--
-- TOC entry 3092 (class 0 OID 0)
-- Dependencies: 203
-- Name: storage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: vos_user
--

ALTER SEQUENCE public.storage_id_seq OWNED BY public.storage.id;


--
-- TOC entry 204 (class 1259 OID 16613)
-- Name: users; Type: TABLE; Schema: public; Owner: vos_user
--

CREATE TABLE public.users (
    username text NOT NULL,
    groupread text[] DEFAULT ARRAY[]::text[],
    groupwrite text[] DEFAULT ARRAY[]::text[],
    password text NOT NULL,
    space_name text NOT NULL,
    admin boolean DEFAULT false NOT NULL
);


ALTER TABLE public.users OWNER TO vos_user;

--
-- TOC entry 205 (class 1259 OID 16622)
-- Name: uws_jobs; Type: TABLE; Schema: public; Owner: vos_user
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


ALTER TABLE public.uws_jobs OWNER TO vos_user;

--
-- TOC entry 2913 (class 2604 OID 16630)
-- Name: space id; Type: DEFAULT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.space ALTER COLUMN id SET DEFAULT nextval('public.space_id_seq'::regclass);


--
-- TOC entry 2916 (class 2604 OID 16631)
-- Name: storage id; Type: DEFAULT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.storage ALTER COLUMN id SET DEFAULT nextval('public.storage_id_seq'::regclass);


--
-- TOC entry 2948 (class 2606 OID 16633)
-- Name: uws_jobs job_id_pk; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.uws_jobs
    ADD CONSTRAINT job_id_pk PRIMARY KEY (id);


--
-- TOC entry 2924 (class 2606 OID 16635)
-- Name: nodes node_id_unique; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT node_id_unique UNIQUE (id);


--
-- TOC entry 2926 (class 2606 OID 16637)
-- Name: nodes node_pk; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT node_pk PRIMARY KEY (path, space_id);


--
-- TOC entry 2928 (class 2606 OID 16639)
-- Name: nodes nodes_unique; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT nodes_unique UNIQUE (path, space_id);


--
-- TOC entry 2933 (class 2606 OID 16641)
-- Name: properties properties_unique; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT properties_unique UNIQUE (uri, node_path, space_id);


--
-- TOC entry 2935 (class 2606 OID 16643)
-- Name: space space_pk; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.space
    ADD CONSTRAINT space_pk PRIMARY KEY (id);


--
-- TOC entry 2937 (class 2606 OID 16645)
-- Name: space space_unique_host_port; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.space
    ADD CONSTRAINT space_unique_host_port UNIQUE (host, port);


--
-- TOC entry 2939 (class 2606 OID 16647)
-- Name: space space_unique_name; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.space
    ADD CONSTRAINT space_unique_name UNIQUE (name);


--
-- TOC entry 2942 (class 2606 OID 16649)
-- Name: storage storage_pk; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.storage
    ADD CONSTRAINT storage_pk PRIMARY KEY (id);


--
-- TOC entry 2944 (class 2606 OID 16651)
-- Name: storage storage_unique; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.storage
    ADD CONSTRAINT storage_unique UNIQUE (name, host, port);


--
-- TOC entry 2946 (class 2606 OID 16653)
-- Name: users user_pk; Type: CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT user_pk PRIMARY KEY (space_name, username);


--
-- TOC entry 2922 (class 1259 OID 16654)
-- Name: fki_space_fk; Type: INDEX; Schema: public; Owner: vos_user
--

CREATE INDEX fki_space_fk ON public.nodes USING btree (space_id);


--
-- TOC entry 2940 (class 1259 OID 16655)
-- Name: fki_storage_fk; Type: INDEX; Schema: public; Owner: vos_user
--

CREATE INDEX fki_storage_fk ON public.storage USING btree (name);


--
-- TOC entry 2949 (class 1259 OID 16656)
-- Name: owner_idx; Type: INDEX; Schema: public; Owner: vos_user
--

CREATE INDEX owner_idx ON public.uws_jobs USING btree (owner bpchar_pattern_ops);


--
-- TOC entry 2929 (class 1259 OID 16657)
-- Name: path_gist_idx; Type: INDEX; Schema: public; Owner: vos_user
--

CREATE INDEX path_gist_idx ON public.nodes USING gist (path);


--
-- TOC entry 2930 (class 1259 OID 16658)
-- Name: path_idx; Type: INDEX; Schema: public; Owner: vos_user
--

CREATE INDEX path_idx ON public.nodes USING btree (path);


--
-- TOC entry 2950 (class 1259 OID 16659)
-- Name: phase_idx; Type: INDEX; Schema: public; Owner: vos_user
--

CREATE INDEX phase_idx ON public.uws_jobs USING btree (phase);


--
-- TOC entry 2931 (class 1259 OID 16660)
-- Name: properties_idx; Type: INDEX; Schema: public; Owner: vos_user
--

CREATE INDEX properties_idx ON public.properties USING btree (node_path);


--
-- TOC entry 2957 (class 2620 OID 16661)
-- Name: uws_jobs delete_trigger; Type: TRIGGER; Schema: public; Owner: vos_user
--

CREATE TRIGGER delete_trigger AFTER DELETE ON public.uws_jobs FOR EACH ROW EXECUTE PROCEDURE public.delete_notify_trigger();


--
-- TOC entry 2958 (class 2620 OID 16662)
-- Name: uws_jobs insert_trigger; Type: TRIGGER; Schema: public; Owner: vos_user
--

CREATE TRIGGER insert_trigger AFTER INSERT OR UPDATE ON public.uws_jobs FOR EACH ROW EXECUTE PROCEDURE public.insert_notify_trigger();


--
-- TOC entry 2956 (class 2620 OID 16663)
-- Name: nodes path_change_trigger; Type: TRIGGER; Schema: public; Owner: vos_user
--

CREATE TRIGGER path_change_trigger BEFORE UPDATE ON public.nodes FOR EACH ROW EXECUTE PROCEDURE public.update_path_modified_column();


--
-- TOC entry 2959 (class 2620 OID 16664)
-- Name: uws_jobs update_trigger; Type: TRIGGER; Schema: public; Owner: vos_user
--

CREATE TRIGGER update_trigger AFTER UPDATE ON public.uws_jobs FOR EACH ROW EXECUTE PROCEDURE public.update_modified_column();


--
-- TOC entry 2953 (class 2606 OID 16665)
-- Name: properties properties_fk; Type: FK CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT properties_fk FOREIGN KEY (node_path, space_id) REFERENCES public.nodes(path, space_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2951 (class 2606 OID 16670)
-- Name: nodes space_fk; Type: FK CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT space_fk FOREIGN KEY (space_id) REFERENCES public.space(id) ON UPDATE SET NULL ON DELETE SET NULL;


--
-- TOC entry 2955 (class 2606 OID 16675)
-- Name: uws_jobs space_fk; Type: FK CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.uws_jobs
    ADD CONSTRAINT space_fk FOREIGN KEY (space_id) REFERENCES public.space(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2954 (class 2606 OID 16680)
-- Name: storage storage_fk; Type: FK CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.storage
    ADD CONSTRAINT storage_fk FOREIGN KEY (name) REFERENCES public.space(name) ON UPDATE CASCADE;


--
-- TOC entry 2952 (class 2606 OID 16685)
-- Name: nodes storage_fk; Type: FK CONSTRAINT; Schema: public; Owner: vos_user
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT storage_fk FOREIGN KEY (storage_id) REFERENCES public.storage(id);


-- Completed on 2018-09-07 14:03:44 AWST

--
-- PostgreSQL database dump complete
--

