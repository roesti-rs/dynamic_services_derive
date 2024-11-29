use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::{self, parse_str, token, Data, DataEnum, DataStruct, DataUnion, DeriveInput, Error, Fields, GenericArgument, PathArguments, Result, Type};
use proc_macro2::Span;

#[derive(Debug)]
enum Action {
    LifeTimes{names: Vec<String>},
    SetterInjectField{field: String, type_name: String},
    ActivatorFunct{func_name: String, arguments: Vec<String>},
    DeactivatorFunct{func_name: String},
    UpdateFunct{func_name: String},
    StructPath{path: String}
}

impl Action {
    fn to_json_string(&self) -> String {
        match self {
            Action::LifeTimes {names} => {
                format!("{{\"op\":\"LifeTimes\", \"names\":{:?} }}", names)
            },
            Action::SetterInjectField{field, type_name} => {
                format!("{{\"op\":\"SetterInjectField\", \"field\":\"{}\", \"type\":\"{}\"}}", field, type_name)
            },
            Action::ActivatorFunct{func_name, arguments} => {
                format!("{{\"op\":\"ActivatorFunct\", \"method\":\"{}\", \"args\":{:?} }}", func_name, arguments)
            },
            Action::DeactivatorFunct{func_name} => {
                format!("{{\"op\":\"DeactivatorFunct\", \"method\":\"{}\"}}", func_name)
            }
            Action::UpdateFunct{func_name} => {
                format!("{{\"op\":\"UpdateFunct\", \"method\":\"{}\"}}", func_name)
            }
            Action::StructPath { path } => {
                format!("{{\"op\":\"StructPath\", \"path\":\"{}\"}}", path)
            }
        }
    }
}

// Implement the #derive(DynamicServices) macro
#[proc_macro_derive(DynamicServices, attributes(inject, constructor))]
pub fn dynamic_services_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Find the injection points and write to file
    find_injection_points(ast)
}

// Find the injection points and write these to a file at target/_<type_name>.tmp
fn find_injection_points(ast: syn::DeriveInput) -> TokenStream {
    let (tn, actions) = match find_injection_info(ast) {
        Ok(t) => t,
        Err(err) => return TokenStream::from(err.to_compile_error())
    };

    let mut lines = vec![];
    lines.push("[".to_string());
    let mut first = true;
    for a in &actions {
        if first {
            first = false;
        } else {
            lines.push(",".to_string());
        }

        lines.push(a.to_json_string());
    }
    lines.push("]".to_string());
    write_actions_file(tn, lines);

    quote!{}.into()
}

// Write a file target/_<type_name>.tmp with the injection actions
fn write_actions_file(tn: String, lines: Vec<String>) {
    if lines.is_empty() {
        return;
    }

    let filenm = format!("{}/target/_{}.tmp", std::env::var("CARGO_MANIFEST_DIR").unwrap(), tn);

    let mut content = lines.join("\n");
    content.push('\n');
    std::fs::write(filenm, content).expect("Unable to write file");
}

// Search the AST for the injection info, which is injected fields and lifetime information
fn find_injection_info(ast: DeriveInput)
  -> Result<(String, Vec<Action>)> {
    let fields = match ast.data {
        | Data::Enum(DataEnum { enum_token: token::Enum { span }, ..})
        | Data::Union(DataUnion { union_token: token::Union { span }, ..})
        => {
            return Err(Error::new(span, "expected a struct"));
        },
        | Data::Struct(DataStruct { fields: Fields::Named(it), .. })
        => {
            it
        },
        | Data::Struct(_)
        => {
            return Err(Error::new(Span::call_site(), "expected a struct with named fields"));
        },
    };

    let mut actions = Vec::new();

    // Find the fields with the #[inject] macro
    for f in fields.named.iter() {
        if !find_attribute(f, "inject") {
            continue;
        }

        if let syn::Type::Path(ref_type) = &f.ty {
            let id = f.ident.as_ref().unwrap();
            if let Some(a) = get_type_name(id, ref_type) {
                actions.push(a);
            }
        }
    }

    actions.push(get_lifetimes(&ast.generics.params));

    Ok((ast.ident.to_string(), actions))
}

// Get the declared lifetimes of the struct as they are needed when generating additions to it
fn get_lifetimes(params: &syn::punctuated::Punctuated<syn::GenericParam, token::Comma>) -> Action {
    let mut lifetimes = vec![];

    for param in params.iter() {
        match param {
            | syn::GenericParam::Lifetime(lt)
            => {
                lifetimes.push(lt.lifetime.ident.to_string());
            },
            | _
            => {}
        }
    }

    Action::LifeTimes { names: lifetimes }
}


fn find_attribute(f: &syn::Field, _name: &str) -> bool {
    for a in &f.attrs {
        if let Some(name) = a.path().get_ident() {
            if name == "inject" {
                return true;
            }
        }
    }
    false
}

//////////////////////////////////////////////////////////////////////////////////
// Methods below here are used to find the fields that need to be injected in the syntax tree
//////////////////////////////////////////////////////////////////////////////////
fn get_type_name(ident: &syn::Ident, ref_type: &syn::TypePath) -> Option<Action> {
    if let Some(s) = ref_type.path.segments.first() {
        if s.ident != "ServiceReference" {
            return None;
        }

        return match &s.arguments {
            | PathArguments::AngleBracketed(aba)
                => get_serviceref_typearg(ident, aba),
            | _ => None
        };
    }

    None
}

fn get_serviceref_typearg(ident: &syn::Ident, aba: &syn::AngleBracketedGenericArguments) -> Option<Action> {
    if let Some(GenericArgument::Type(Type::Path(tp))) = aba.args.first() {
        if let Some(tn) = tp.path.segments.first() {
            return Some(Action::SetterInjectField { field: ident.to_string(), type_name: tn.ident.to_string() });
        }
    }

    None
}
//////////////////////////////////////////////////////////////////////////////////
// Methods above here are used to find the fields that need to be injected in the syntax tree
//////////////////////////////////////////////////////////////////////////////////

#[proc_macro_attribute]
pub fn activator(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Nothing to do, as this is handled by the #dynamic_services macro
    item
}

#[proc_macro_attribute]
pub fn deactivator(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Nothing to do, as this is handled by the #dynamic_services macro
    item
}

#[proc_macro_attribute]
pub fn update(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Nothing to do, as this is handled by the #dynamic_services macro
    item
}

// This macro is used on struct implementations to generate the code for the dynamic services.
// It can only be used on structs that have the #[derive(DynamicServices)] macro.
#[proc_macro_attribute]
pub fn dynamic_services(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let toks: Result<syn::ItemImpl> = syn::parse(item.clone());
    let tokens = toks.unwrap();

    // Read the type name from the implementation
    let impl_type_box = &tokens.self_ty;
    let impl_type = if let Type::Path(tp) = impl_type_box.as_ref() {
        tp.path.segments.first().unwrap()
    } else {
        panic!("Not a path");
    };
    let type_name = impl_type.ident.to_string();

    // Find the various macros in the implementation code, #[activator], #[deactivator],
    // #[update] that denote the callback methods to use.
    if let Some(activator) = find_activator(&tokens) {
        write_action(activator, &type_name, "acttmp");
    }
    if let Some(deactivator) = find_deactivator(&tokens) {
        write_action(deactivator, &type_name, "deacttmp");
    }
    if let Some(update) = find_update(&tokens) {
        write_action(update, &type_name, "updtmp");
    }
    if let Some(path) = get_struct_path(attrs) {
        write_action(path, &type_name, "pathtmp");
    }

    let mut generated: proc_macro2::TokenStream = item.into();

    let file = format!("{}/target/_{}.tmp", std::env::var("CARGO_MANIFEST_DIR").unwrap(), type_name);
    if Path::new(&file).exists() {
        generate_impl(&file, &type_name, &mut generated);
    } // TODO fail if not found

    generated.into()
}

fn get_struct_path(attrs: TokenStream) -> Option<Action> {
    let args_parsed = syn::punctuated::Punctuated::<syn::ExprAssign, syn::Token![,]>::parse_terminated.parse(attrs).unwrap();
    for arg in args_parsed.iter() {
        if let syn::Expr::Path(key) = arg.left.as_ref() {
            if let Some(ps) = key.path.segments.first() {
                if ps.ident == "path" {
                    if let syn::Expr::Path(p) = arg.right.as_ref() {
                        return get_full_path(p);
                    }
                }
            }
        }
    }
    None
}

fn get_full_path(path: &syn::ExprPath) -> Option<Action> {
    let mut strings = vec![];

    for el in path.path.segments.iter() {
        strings.push(el.ident.to_string());
    }
    if !strings.is_empty() {
        return Some(Action::StructPath { path: strings.join("::") });
    }
    None
}

// Write file in target/_<curtype>.<suffix> with the action serialized as JSON
fn write_action(action: Action, curtype: &str, suffix: &str) {
    let filenm = format!("{}/target/_{}.{}", std::env::var("CARGO_MANIFEST_DIR").unwrap(), curtype, suffix);
    let content = format!("[{}]", action.to_json_string());
    std::fs::write(filenm, content).unwrap();
}

fn find_lifecycle_callback(ls: &str, itimpl: &syn::ItemImpl) -> Option<(String, Vec<String>)> {
    for item in itimpl.items.iter() {
        if let syn::ImplItem::Fn(f) = item {
            for a in f.attrs.iter() {
                if let Some(an) = a.meta.path().get_ident() {
                    if *an == ls {
                        let inputs = get_inputs_from_fn(&f.sig.inputs);
                        return Some((f.sig.ident.to_string(), inputs));
                    }
                }
            }
        }
    };

    None
}

fn find_activator(itimpl: &syn::ItemImpl) -> Option<Action> {
    let act = find_lifecycle_callback("activator", itimpl);
    if let Some((name, args)) = act {
        return Some(Action::ActivatorFunct { func_name: name, arguments: args });
    }
    None
}

fn find_deactivator(itimpl: &syn::ItemImpl) -> Option<Action> {
    let deact = find_lifecycle_callback("deactivator", itimpl);
    if let Some((name, _)) = deact {
        return Some(Action::DeactivatorFunct { func_name: name });
    }
    None
}

fn find_update(itimpl: &syn::ItemImpl) -> Option<Action> {
    let upd = find_lifecycle_callback("update", itimpl);
    if let Some((name, _)) = upd {
        return Some(Action::UpdateFunct { func_name: name });
    }
    None
}

// Get the parameter signatures from a callback function signature
fn get_inputs_from_fn(inputs: &syn::punctuated::Punctuated<syn::FnArg, token::Comma>) -> Vec<String> {
    let mut args = vec![];

    for (counter, input) in inputs.iter().enumerate() {
        match input {
            | syn::FnArg::Receiver(_r)
            => {
                if counter > 0 {
                    panic!("Only the first argument should be a Self reference");
                }
            },
            | syn::FnArg::Typed(arg)
            => {
                if counter == 0 {
                    panic!("The first argument should be a Self reference");
                }
                if let syn::Type::Reference(tr) = arg.ty.as_ref() {
                    if let syn::Type::Path(tp) = tr.elem.as_ref() {
                        if let Some(tn) = tp.path.segments.first() {
                            args.push(format!("&{}", tn.ident));
                        }
                    }
                }
            }
        }
    }

    args
}

// Generate the extension to the struct impl
fn generate_impl(file_path: &str, type_name: &str, generated: &mut proc_macro2::TokenStream) {
    let content = fs::read_to_string(file_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    let lifetimes = get_lifetimes_from_json(json.as_array().unwrap());
    let mut fields = vec![];
    for action in json.as_array().unwrap() {
        // Iterate over all injection points
        generated.extend(generate_action(type_name, action, &mut fields, &lifetimes));
    }

    if !fields.is_empty() {
        generate_unset_all(type_name, lifetimes, fields, generated);
    }
}

fn generate_unset_all(type_name: &str, lifetimes: Vec<String>, fields: Vec<String>,
        generated: &mut proc_macro2::TokenStream) {
    let tn = format_ident!("{}", type_name);
    let lifetimes_code = quote_fixed_lifetimes(lifetimes.len(), quote! { '_ });
    let mut unset_calls = vec![];
    for field in fields {
        let injected_ref = format_ident!("{}", field);
        unset_calls.push(quote! {
            self.#injected_ref = ServiceReference::default();
        });
    }
    generated.extend(quote!{
        impl #tn #lifetimes_code {
            pub fn unset_all(&mut self) {
                println!("[{}] Unsetting all injected fields", #type_name);
                #(#unset_calls)*
            }
        }
    });
}

fn get_lifetimes_from_json(actions: &[serde_json::Value]) -> Vec<String> {
    let mut lifetimes = vec![];

    for action in actions {
        let op = action["op"].as_str().unwrap();
        if op == "LifeTimes" {
            if let Some(names) = action["names"].as_array() {
                names.iter().for_each(|v| {
                    if let Some(lt) = v.as_str() {
                        lifetimes.push(lt.to_string());
                    }
                });
            }
        }
    }
    lifetimes
}

fn generate_update_method(type_name: &str, injected_type_name: &str,
    field: &str) -> proc_macro2::TokenStream {
    // Read the target/_<type_name>.updtmp file to get the update method name
    let mut upd_func_names = vec![];
    let upd_file = format!("{}/target/_{}.updtmp", std::env::var("CARGO_MANIFEST_DIR").unwrap(), type_name);
    if Path::new(&upd_file).exists() {
        let upd_content = fs::read_to_string(upd_file).unwrap();
        let json: serde_json::Value = serde_json::from_str(&upd_content).unwrap();
        for action in json.as_array().unwrap() {
            let op = action["op"].as_str().unwrap();
            match op {
                "UpdateFunct" => {
                    let func_name = action["method"].as_str().unwrap();
                    upd_func_names.push(func_name.to_string());
                }
                _ => {
                    panic!("Unknown action: {}", op);
                }
            }
            // alternative
            // if op == "UpdateFunct" {
            //     upd_func_name.push(action["method"].as_str().unwrap().to_string());
            // }
        }
    }

    // For each update method name found generate code to call the update method.
    // If there are no update methods then this will generate no code.
    let mut update_calls = vec![];
    for name in upd_func_names {
        let func_name = format_ident!("{}", name);
        update_calls.push(quote!{
            self.#func_name(#field, props.clone());
        });
    }

    // Generate the update_<injected_type_name> method
    let update_type = format_ident!("update_{}", injected_type_name);
    let set_type_ref = format_ident!("set_{}_ref", injected_type_name);
    let new_code = quote!{
        #[allow(non_snake_case)]
        pub fn #update_type(&mut self,
                sreg: &::dynamic_services::ServiceRegistration,
                props: &std::collections::BTreeMap<String, String>) {
            self.#set_type_ref(&sreg, &props);

            #(#update_calls)*
        }
    };

    new_code
}

// Generate the consumer impl code for actions found in the target/_<type_name>.tmp file
fn generate_action(type_name: &str, action: &serde_json::Value, fields: &mut Vec<String>,
        lifetimes: &[String]) -> proc_macro2::TokenStream {
    let lifetimes_code = quote_fixed_lifetimes(lifetimes.len(), quote! { '_ });

    let op = action["op"].as_str().unwrap();
    match op {
        "SetterInjectField" => {
            // Generate setters/getters/updaters
            let field = action["field"].as_str().unwrap();
            fields.push(field.to_string());
            let injected_type_name = action["type"].as_str().unwrap();

            let tn = format_ident!("{}", type_name);
            let get_ts_ref = format_ident!("get_{}_ref", injected_type_name);
            let get_ts_field = format_ident!("get_{}_fieldname", injected_type_name);
            let set_ts_ref = format_ident!("set_{}_ref", injected_type_name);
            let itn = format_ident!("{}", injected_type_name);
            let injected_ref = format_ident!("{}", field);

            let update_md = generate_update_method(type_name, injected_type_name, field);

            let new_code = quote! {
                impl #tn #lifetimes_code {
                    // Generate the getter for the injected field
                    #[allow(non_snake_case)]
                    pub fn #get_ts_ref(&self) -> &ServiceReference<#itn> {
                        &self.#injected_ref
                    }

                    // Returns the name of the injected field as string
                    #[allow(non_snake_case)]
                    pub fn #get_ts_field() -> &'static str {
                        #field
                    }

                    // Generate the setter for the injected field
                    #[allow(non_snake_case)]
                    pub fn #set_ts_ref(&mut self,
                            sreg: &::dynamic_services::ServiceRegistration,
                            props: &std::collections::BTreeMap<String, String>) {
                        self.#injected_ref = ServiceReference::from(sreg, props.clone());
                    }

                    #update_md
                }
            };

            new_code
        },
        "LifeTimes" => {
            // read earlier
            quote!{} // returns no code
        },
        _ => {
            panic!("Unknown action: {}", op);
        }
    }
}

// This macro is for the main implementation. It expands its code with additional functions.
#[proc_macro_attribute]
pub fn dynamic_services_main(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut generated: proc_macro2::TokenStream = item.into();

    let new_code = quote! {
        fn register_service(svc: Box<dyn ::std::any::Any + Send + Sync>,
                mut props: std::collections::BTreeMap<String, String>)
                -> ::dynamic_services::ServiceRegistration {
            register_consumers();

            let sreg = ::dynamic_services::ServiceRegistration::new();
            props.insert(".service_id".to_string(), sreg.id.to_string());
            ::dynamic_services::REGD_SERVICES.write().unwrap().insert(sreg.clone(), (svc, props));

            inject_consumers();
            sreg
        }

        fn update_service(sreg: &::dynamic_services::ServiceRegistration,
                mut props: std::collections::BTreeMap<String, String>) {
            props.insert(".service_id".to_string(), sreg.id.to_string());

            {
                // In a separate scope to not keep the write lock scope short
                let mut regd = ::dynamic_services::REGD_SERVICES.write().unwrap();
                if let Some((_, p)) = regd.get_mut(sreg) {
                    *p = props.clone();

                }
            }
            update_consumers(sreg, props);
        }

        fn unregister_service(sr: ::dynamic_services::ServiceRegistration) {
            if ::dynamic_services::REGD_SERVICES.write().unwrap().remove(&sr).is_some() {
                println!("Service unregistered: {:?}", sr);
                uninject_consumers(&sr);
            }
        }
    };
    generated.extend(new_code);

    let mut consumer_types = HashMap::new();
    let dir = format!("{}/target", std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let paths = fs::read_dir(dir).unwrap();
    for path in paths.flatten() {
        if let Some((name, path, tokens)) =
                generate_consumer(path.path(), path.file_name().to_str().unwrap()) {
            consumer_types.insert(name, path);
            generated.extend(tokens);
        }
    }

    // Generate stable methods with a fixed name that call out to all relevant consumers
    generated.extend(generate_register_consumers(&consumer_types));
    generated.extend(generate_inject_consumers(&consumer_types));
    generated.extend(generate_update_consumers(&consumer_types));
    generated.extend(generate_uninject_consumers(&consumer_types));

    generated.into()
}

fn quote_fixed_lifetimes(num: usize, lt: proc_macro2::TokenStream ) -> proc_macro2::TokenStream {
    let lifetimes = vec![lt; num];
    quote! { <#(#lifetimes),*> }
}

/* Generates a 2 (lazy) statics
 * CONSUMER_CTOR_<TYPE> which contains the constructor function that can be called to create the consumer
 * CONSUMER_INST_<TYPE> which is a map with as key ConsumerRegistration and value a tuple of
 *   (the_consumer_instance, a vector of service regstrations used, metadata)
 */
fn generate_consumer(path: PathBuf, file_name: &str) -> Option<(String, String, proc_macro2::TokenStream)> {
    if file_name.starts_with("_") && file_name.ends_with(".tmp") {
        let content = fs::read_to_string(&path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();
        let lifetimes = get_lifetimes_from_json(json.as_array().unwrap());
        let static_lifetimes = quote_fixed_lifetimes(lifetimes.len(), quote! { 'static });

        let type_name = &file_name[1..file_name.len()-4];

        let mut path_file = path.clone();
        path_file.pop();
        path_file.push(format!("_{}.pathtmp", type_name));
        let path = if path_file.exists() {
            let content = fs::read_to_string(path_file).unwrap();
            let json: serde_json::Value = serde_json::from_str(&content).unwrap();
            if let Some(p) = get_path_from_json(json.as_array().unwrap()) {
                p
            } else {
                type_name.to_string()
            }
        } else {
            type_name.to_string()
        };

        let fqn = format!("{}::{}", path, type_name);
        let ps: syn::ExprPath = parse_str(&fqn).unwrap();

        let register_fn = format_ident!("register_{}", type_name);
        let global_ctor_map = format_ident!("CONSUMER_CTOR_{}", type_name.to_uppercase());
        let global_inst_map = format_ident!("CONSUMER_INST_{}", type_name.to_uppercase());

        let inject_function = generate_inject_function(json, type_name);

        let tokens = quote!{
            static #global_ctor_map: ::once_cell::sync::Lazy<std::sync::RwLock<Vec<fn() -> #ps #static_lifetimes>>>
                = ::once_cell::sync::Lazy::new(||std::sync::RwLock::new(Vec::new()));
            static #global_inst_map: ::once_cell::sync::Lazy<std::sync::RwLock<
                    std::collections::HashMap<::dynamic_services::ConsumerRegistration,
                        (#ps, Vec<::dynamic_services::ServiceRegistration>, ::dynamic_services::InjectMetadata)>>>
                = ::once_cell::sync::Lazy::new(||std::sync::RwLock::new(std::collections::HashMap::new()));

            #[allow(non_snake_case)]
            fn #register_fn() {
                println!("Registering Consumer: {}", #type_name);
                #global_ctor_map.write().unwrap().push(|| #ps::default());
            }

            #(#inject_function)*
        };
        return Some((type_name.to_string(), path, tokens));
    }
    None
}

fn get_path_from_json(actions: &[serde_json::Value]) -> Option<String> {
    for action in actions {
        let op = action["op"].as_str().unwrap();
        if op == "StructPath" {
            return Some(action["path"].as_str().unwrap().to_string());
        }
    }
    None
}

fn generate_inject_function(json: serde_json::Value, type_name: &str) -> Vec<proc_macro2::TokenStream> {
    let mut setter_injects = HashMap::new();
    for action in json.as_array().unwrap() {
        let op = action["op"].as_str().unwrap();
        match op {
            "SetterInjectField" => {
                setter_injects.insert(action["field"].as_str().unwrap().to_string(),
                    action["type"].as_str().unwrap().to_string());
            },
            "LifeTimes" => {
                // ignore
            },
            _ => {
                panic!("Unknown action: {}", op);
            }
        }
    }

    // Get the activator invocation, made when all the the injections are done
    let act_call = generate_activator_call(type_name);
    let deact_call = generate_deactivator_call(type_name);

    let mut quotes = vec![];
    if !setter_injects.is_empty() {
        let global_inst_map = format_ident!("CONSUMER_INST_{}", type_name.to_uppercase());

        let expected_num_injects = setter_injects.len();
        let mut inject_calls = vec![];
        for injected_type_name in setter_injects.values() {
            let itn = format_ident!("{}", injected_type_name);
            let getter_ref = format_ident!("get_{}_ref", injected_type_name);
            let setter_ref = format_ident!("set_{}_ref", injected_type_name);

            inject_calls.push(quote!{
                if let Some(sr) = svc.downcast_ref::<#itn>() {
                    for (_, (i, _, md)) in gm.iter_mut() {
                        if i.#getter_ref().get_properties().is_none() {
                            i.#setter_ref(sreg, props);
                            md.inc_fields_injected();
                        }
                    }
                }
            });
        }

        let mut update_calls = vec![];
        for injected_type_name in setter_injects.values() {
            let itn = format_ident!("{}", injected_type_name);
            let updater = format_ident!("update_{}", injected_type_name);
            update_calls.push(quote!{
                if let Some(dcsvc) = svcx.downcast_ref::<#itn>() {
                    for (_, (i, _, _)) in gm.iter_mut() {
                        i.#updater(sreg, propsx);
                    }
                }
            });
        }

        let inject_fn = format_ident!("inject_{}", type_name);
        let update_fn = format_ident!("update_{}", type_name);
        let uninject_fn = format_ident!("uninject_{}", type_name);
        let global_ctor_map = format_ident!("CONSUMER_CTOR_{}", type_name.to_uppercase());
        let q = quote! {
            #[allow(non_snake_case)]
            fn #inject_fn(svc: &Box<dyn ::std::any::Any + Send + Sync>,
                sreg: &::dynamic_services::ServiceRegistration,
                props: &std::collections::BTreeMap<String, String>) {
                let mut gm = #global_inst_map.write().unwrap();
                if gm.is_empty() {
                    for ctor in #global_ctor_map.read().unwrap().iter() {
                        let mut i = ctor();
                        let regs = vec![sreg.clone()];
                        gm.insert(
                            ::dynamic_services::ConsumerRegistration::new(),
                            (i, regs, ::dynamic_services::InjectMetadata::new())
                        );
                    }
                } else {
                    for ctor in #global_ctor_map.read().unwrap().iter() {
                        for (_, (_, regs, _)) in gm.iter_mut() {
                            if !regs.contains(sreg) {
                                regs.push(sreg.clone());
                            }
                        }
                    }
                }
                #(#inject_calls)*

                for (_, (c, regs, md)) in gm.iter_mut() {
                    if md.get_fields_injected() == #expected_num_injects
                        && !md.is_activated() {
                        #act_call;
                        md.set_activated();
                    }
                }
            }

            // Called when the service registration properties are updated
            #[allow(non_snake_case)]
            fn #update_fn(sreg: &::dynamic_services::ServiceRegistration,
                props: &std::collections::BTreeMap<String, String>) {

                let regd = ::dynamic_services::REGD_SERVICES.read().unwrap();
                let svc = regd.get(&sreg);
                if let Some((svcx, propsx)) = svc {
                    let mut gm = #global_inst_map.write().unwrap();

                    #(#update_calls)*
                }
            }

            #[allow(non_snake_case)]
            fn #uninject_fn(sreg: &::dynamic_services::ServiceRegistration) {
                let mut deleted = vec![];
                let mut global = #global_inst_map.write().unwrap();
                global.iter_mut()
                    .filter(|(_, (_, regs, _))| regs.contains(sreg))
                    .for_each(|(ci, (c, _, _))| {
                        deleted.push(ci.clone());
                        c.unset_all();
                        #deact_call;
                    });
                deleted.iter().for_each(|ci| { global.remove(ci); });
            }
        };
        quotes.push(q);
    }

    quotes
}

fn generate_activator_call(type_name: &str) -> proc_macro2::TokenStream {
    let mut new_code = quote! {};

    let file = format!("{}/target/_{}.acttmp", std::env::var("CARGO_MANIFEST_DIR").unwrap(), type_name);
    if Path::new(&file).exists() {
        generate_activator(&file, &mut new_code);
    }

    new_code
}

// Generate the activator call, based on the fact that there are potentially multiple arguments which are references to the service instances
// that the consumer depends on. These can be in any order and can be a subset of all the injected services.
fn generate_activator(file: &str, new_code: &mut proc_macro2::TokenStream) {
    let acttmp_content = fs::read_to_string(file).unwrap();
    let json: serde_json::Value = serde_json::from_str(&acttmp_content).unwrap();

    for action in json.as_array().unwrap() {
        let op = action["op"].as_str().unwrap();
        match op {
            "ActivatorFunct" => {
                let func_name = action["method"].as_str().unwrap();

                let args = action["args"].as_array().unwrap();
                let mut arg_calls = vec![];
                let mut arg_coll_code = vec![quote!{let svc_registry = ::dynamic_services::REGD_SERVICES.read().unwrap();}];
                let mut arg_prep = vec![];
                let mut invoke_cond = vec![quote!{ true }];

                for (argnum, arg) in args.iter().enumerate() {
                    let a = arg.as_str().unwrap();
                    if !a.starts_with('&') {
                        panic!("Expected reference argument");
                    }

                    let arg_name = format_ident!("arg{}", argnum);
                    let arg_type = format_ident!("{}", &a[1..]);
                    let code = quote!{

                        let mut #arg_name = None;
                        for reg in regs.clone() {
                            let (svc, _) = svc_registry.get(&reg).unwrap();

                            if let Some(sr) = svc.downcast_ref::<#arg_type>() {
                                #arg_name = Some(sr);
                            }
                        }
                    };
                    let argval_name = format_ident!("argval{}", argnum);
                    invoke_cond.push(quote!{ #arg_name.is_some() });
                    arg_prep.push(quote!{ let #argval_name = #arg_name.unwrap(); });
                    arg_calls.push(quote!{ #argval_name });
                    arg_coll_code.push(code);
                }

                let activate_md = format_ident!("{}", func_name);
                new_code.extend(quote! {
                    #(#arg_coll_code)*

                    if #(#invoke_cond)&&* {
                        #(#arg_prep)*
                        c.#activate_md(#(#arg_calls),*);
                    }
                });
            },
            _ => {
                panic!("Unknown action: {}", op);
            }
        }
    }
}

fn generate_deactivator_call(type_name: &str) -> proc_macro2::TokenStream {
    let mut new_code = quote! {};

    let file = format!("{}/target/_{}.deacttmp", std::env::var("CARGO_MANIFEST_DIR").unwrap(), type_name);
    if Path::new(&file).exists() {
        generate_deactivator(&file, &mut new_code);
    }

    new_code
}

fn generate_deactivator(file: &str, new_code: &mut proc_macro2::TokenStream) {
    let deacttmp_content = fs::read_to_string(file).unwrap();
    let json: serde_json::Value = serde_json::from_str(&deacttmp_content).unwrap();

    for action in json.as_array().unwrap() {
        let op = action["op"].as_str().unwrap();
        match op {
            "DeactivatorFunct" => {
                let func_name = action["method"].as_str().unwrap();
                let deactivate_md = format_ident!("{}", func_name);
                new_code.extend(quote! {
                    c.#deactivate_md();
                });
            },
            _ => {
                panic!("Unknown action: {}", op);
            }
        }
    }
}

// Generates a register_consumers function that registers each consumer
fn generate_register_consumers(consumer_types: &HashMap<String, String>) -> proc_macro2::TokenStream {
    let mut register_calls = vec![];
    for ct in consumer_types.keys() {
        let register_fn = format_ident!("register_{}", ct);
        register_calls.push(quote!{
            #register_fn();
        });
    }

    let new_code = quote! {
        static CONSUMERS_INITIALIZED: ::std::sync::atomic::AtomicBool =
            ::std::sync::atomic::AtomicBool::new(false);
        fn register_consumers() {
            let initialized = CONSUMERS_INITIALIZED.swap(true, ::std::sync::atomic::Ordering::SeqCst);
            if initialized {
                return;
            }

            #(#register_calls)*
        }
    };
    new_code
}

fn generate_inject_consumers(consumer_types: &HashMap<String, String>) -> proc_macro2::TokenStream {
    let mut inject_calls = vec![];
    for ct in consumer_types.keys() {
        let inject_fn = format_ident!("inject_{}", ct);
        inject_calls.push(quote!{
            #inject_fn(svc, &sreg, &props);
        });
    }

    let new_code = quote! {
        // TODO only inject the relevant consumers and don't re-inject
        fn inject_consumers() {
            for (sreg, (svc, props)) in ::dynamic_services::REGD_SERVICES.read().unwrap().iter() {
                #(#inject_calls)*
            }
        }
    };
    new_code
}

fn generate_uninject_consumers(consumer_types: &HashMap<String, String>) -> proc_macro2::TokenStream {
    // All consumers have in their global map as a value the list in dependent service
    // references. Un-inject all consumers that have the service reference of the service
    // being unregistered.

    let mut uninject_calls = vec![];
    for ct in consumer_types.keys() {
        let uninject_fn = format_ident!("uninject_{}", ct);
        uninject_calls.push(quote!{
            #uninject_fn(sr);
        });
    }

    quote! {
        fn uninject_consumers(sr: &::dynamic_services::ServiceRegistration) {
            #(#uninject_calls)*
        }
    }
}

fn generate_update_consumers(consumer_types: &HashMap<String, String>) -> proc_macro2::TokenStream {
    let mut update_calls = vec![];
    for ct in consumer_types.keys() {
        let update_fn = format_ident!("update_{}", ct);
        update_calls.push(quote!{
            #update_fn(sreg, &props);
        });
    }

    quote! {
        fn update_consumers(sreg: &::dynamic_services::ServiceRegistration,
            props: std::collections::BTreeMap<String, String>) {
            #(#update_calls)*
        }
    }
}
