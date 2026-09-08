use crate::error::EmulationError;

use super::{Emulation, PointerSide, error::WlrootsEmulationCreationError};
use async_trait::async_trait;
use bitflags::bitflags;
use std::collections::HashMap;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use wayland_client::WEnum;
use wayland_client::backend::WaylandError;

use wayland_client::protocol::wl_keyboard::{self, WlKeyboard};
use wayland_client::protocol::wl_output::{self, WlOutput};
use wayland_client::protocol::wl_pointer::{Axis, AxisSource, ButtonState};
use wayland_client::protocol::wl_registry::{self, WlRegistry};
use wayland_client::protocol::wl_seat::{self, WlSeat};
use wayland_protocols_wlr::virtual_pointer::v1::client::{
    zwlr_virtual_pointer_manager_v1::ZwlrVirtualPointerManagerV1 as VpManager,
    zwlr_virtual_pointer_v1::ZwlrVirtualPointerV1 as Vp,
};

use wayland_protocols_misc::zwp_virtual_keyboard_v1::client::{
    zwp_virtual_keyboard_manager_v1::ZwpVirtualKeyboardManagerV1 as VkManager,
    zwp_virtual_keyboard_v1::ZwpVirtualKeyboardV1 as Vk,
};

use wayland_protocols::xdg::xdg_output::zv1::client::{
    zxdg_output_manager_v1::ZxdgOutputManagerV1,
    zxdg_output_v1::{self, ZxdgOutputV1},
};

use wayland_client::{
    Connection, Dispatch, EventQueue, QueueHandle, delegate_noop,
    globals::{Global, GlobalList, GlobalListContents, registry_queue_init},
};

use input_event::{Event, KeyboardEvent, PointerEvent, scancode};

use super::EmulationHandle;
use super::error::WaylandBindError;

/// Logical geometry of one output, resolved from xdg_output. `position` and
/// `size` are in the compositor's global *logical* coordinate space (already
/// scale-adjusted), which is the same space `motion`/`motion_absolute` on a
/// virtual pointer use.
#[derive(Clone, Debug, Default)]
struct OutputGeom {
    name: String,
    position: (i32, i32),
    size: (i32, i32),
}

#[derive(Clone, Debug)]
struct Output {
    wl_output: WlOutput,
    global_name: u32,
    has_geom: bool,
    pending: OutputGeom,
    geom: OutputGeom,
}

impl Output {
    fn new(wl_output: WlOutput, global_name: u32) -> Self {
        Self {
            wl_output,
            global_name,
            has_geom: false,
            pending: OutputGeom::default(),
            geom: OutputGeom::default(),
        }
    }
}

struct State {
    keymap: Option<(u32, OwnedFd, u32)>,
    input_for_client: HashMap<EmulationHandle, VirtualInput>,
    seat: wl_seat::WlSeat,
    qh: QueueHandle<Self>,
    vpm: VpManager,
    vkm: VkManager,
    global_list: GlobalList,
    xdg_output_manager: ZxdgOutputManagerV1,
    outputs: Vec<Output>,
}

// App State, implements Dispatch event handlers
pub(crate) struct WlrootsEmulation {
    last_flush_failed: bool,
    state: State,
    queue: EventQueue<State>,
}

impl WlrootsEmulation {
    pub(crate) fn new() -> Result<Self, WlrootsEmulationCreationError> {
        let conn = Connection::connect_to_env()?;
        let (global_list, queue) = registry_queue_init::<State>(&conn)?;
        let qh = queue.handle();

        let seat: wl_seat::WlSeat = global_list
            .bind(&qh, 7..=8, ())
            .map_err(|e| WaylandBindError::new(e, "wl_seat 7..=8"))?;

        let vpm: VpManager = global_list
            .bind(&qh, 1..=1, ())
            .map_err(|e| WaylandBindError::new(e, "wlr-virtual-pointer-unstable-v1"))?;
        let vkm: VkManager = global_list
            .bind(&qh, 1..=1, ())
            .map_err(|e| WaylandBindError::new(e, "virtual-keyboard-unstable-v1"))?;
        let xdg_output_manager: ZxdgOutputManagerV1 = global_list
            .bind(&qh, 1..=3, ())
            .map_err(|e| WaylandBindError::new(e, "xdg_output_manager 1..=3"))?;

        let input_for_client: HashMap<EmulationHandle, VirtualInput> = HashMap::new();

        let mut emulate = WlrootsEmulation {
            last_flush_failed: false,
            state: State {
                keymap: None,
                input_for_client,
                seat,
                vpm,
                vkm,
                qh,
                global_list,
                xdg_output_manager,
                outputs: Vec::new(),
            },
            queue,
        };
        // register outputs that are already present
        for global in emulate.state.global_list.contents().clone_list() {
            emulate.state.register_global(global);
        }
        while emulate.state.keymap.is_none() {
            emulate.queue.blocking_dispatch(&mut emulate.state)?;
        }
        // The wlroots emulation backend is request-driven: after creation it
        // only flushes the queue and never dispatches incoming events, so the
        // xdg_output geometry events for the outputs registered above would
        // never arrive. Do a round-trip now to collect each output's logical
        // geometry, which is required for the seam pointer warp.
        emulate.queue.roundtrip(&mut emulate.state)?;
        // let fd = unsafe { &File::from_raw_fd(emulate.state.keymap.unwrap().1.as_raw_fd()) };
        // let mmap = unsafe { MmapOptions::new().map_copy(fd).unwrap() };
        // log::debug!("{:?}", &mmap[..100]);
        Ok(emulate)
    }
}

impl State {
    fn register_global(&mut self, global: Global) {
        if global.interface == "wl_output" {
            let wl_output = self.global_list.registry().bind::<WlOutput, _, _>(
                global.name,
                4,
                &self.qh,
                global.name,
            );
            self.xdg_output_manager
                .get_xdg_output(&wl_output, &self.qh, global.name);
            self.outputs.push(Output::new(wl_output, global.name));
        }
    }

    fn deregister_global(&mut self, name: u32) {
        self.outputs.retain(|o| {
            if o.global_name == name {
                o.wl_output.release();
                false
            } else {
                true
            }
        });
    }

    /// Finalize the geometry of the output identified by the wl_output global
    /// `name` once its xdg_output info has fully arrived.
    fn update_output_info(&mut self, name: u32) {
        let Some(output) = self.outputs.iter_mut().find(|o| o.global_name == name) else {
            return;
        };
        if !output.has_geom {
            output.has_geom = true;
        }
        output.geom = output.pending.clone();
        log::info!(
            "wlroots emulation output {} at ({},{}) {}x{}",
            output.geom.name,
            output.geom.position.0,
            output.geom.position.1,
            output.geom.size.0,
            output.geom.size.1
        );
    }

    fn add_client(&mut self, client: EmulationHandle) {
        let pointer: Vp = self.vpm.create_virtual_pointer(None, &self.qh, ());
        let keyboard: Vk = self.vkm.create_virtual_keyboard(&self.seat, &self.qh, ());

        // TODO: use server side keymap
        if let Some((format, fd, size)) = self.keymap.as_ref() {
            keyboard.keymap(*format, fd.as_fd(), *size);
        } else {
            panic!("no keymap");
        }

        let vinput = VirtualInput {
            pointer,
            keyboard,
            modifiers: Arc::new(Mutex::new(XMods::empty())),
        };

        self.input_for_client.insert(client, vinput);
    }

    fn destroy_client(&mut self, handle: EmulationHandle) {
        if let Some(input) = self.input_for_client.remove(&handle) {
            input.pointer.destroy();
            input.keyboard.destroy();
        }
    }
}

/// Choose a point just inside the outer edge of the monitor that is outermost
/// on `side`, plus the global logical layout bounds. Returns
/// `(target, (min_x, min_y), (max_x, max_y))` in the compositor's global
/// logical coordinate space. `margin` keeps the warp a few pixels inside the
/// very edge so it does not immediately trip the 1-px capture strip that
/// returns the pointer to the peer.
#[allow(clippy::type_complexity)]
fn seam_target(
    geoms: &[OutputGeom],
    side: PointerSide,
    margin: i32,
) -> Option<((i32, i32), (i32, i32), (i32, i32))> {
    if geoms.is_empty() {
        return None;
    }
    let min_x = geoms.iter().map(|g| g.position.0).min()?;
    let min_y = geoms.iter().map(|g| g.position.1).min()?;
    let max_x = geoms.iter().map(|g| g.position.0 + g.size.0).max()?;
    let max_y = geoms.iter().map(|g| g.position.1 + g.size.1).max()?;

    let target = match side {
        PointerSide::Left => {
            let m = geoms.iter().min_by_key(|g| g.position.0)?;
            (min_x + margin, m.position.1 + m.size.1 / 2)
        }
        PointerSide::Right => {
            let m = geoms.iter().max_by_key(|g| g.position.0 + g.size.0)?;
            (
                m.position.0 + m.size.0 - margin,
                m.position.1 + m.size.1 / 2,
            )
        }
        PointerSide::Top => {
            let m = geoms.iter().min_by_key(|g| g.position.1)?;
            (m.position.0 + m.size.0 / 2, min_y + margin)
        }
        PointerSide::Bottom => {
            let m = geoms.iter().max_by_key(|g| g.position.1 + g.size.1)?;
            (
                m.position.0 + m.size.0 / 2,
                m.position.1 + m.size.1 - margin,
            )
        }
    };
    Some((target, (min_x, min_y), (max_x, max_y)))
}

#[async_trait]
impl Emulation for WlrootsEmulation {
    async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        if let Some(virtual_input) = self.state.input_for_client.get(&handle) {
            if self.last_flush_failed {
                match self.queue.flush() {
                    Err(WaylandError::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                        /*
                         * outgoing buffer is full - sending more events
                         * will overwhelm the output buffer and leave the
                         * wayland connection in a broken state
                         */
                        log::warn!("can't keep up, discarding event: ({handle}) - {event:?}");
                        return Ok(());
                    }
                    _ => {}
                }
            }
            virtual_input
                .consume_event(event)
                .unwrap_or_else(|_| panic!("failed to convert event: {event:?}"));
            match self.queue.flush() {
                Err(WaylandError::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.last_flush_failed = true;
                    log::warn!("can't keep up, discarding event: ({handle}) - {event:?}");
                }
                Err(WaylandError::Protocol(e)) => panic!("wayland protocol violation: {e}"),
                Ok(()) => self.last_flush_failed = false,
                Err(e) => Err(e)?,
            }
        }
        Ok(())
    }

    async fn create(&mut self, handle: EmulationHandle) {
        self.state.add_client(handle);
        if let Err(e) = self.queue.flush() {
            log::error!("{e}");
        }
    }
    async fn destroy(&mut self, handle: EmulationHandle) {
        self.state.destroy_client(handle);
        if let Err(e) = self.queue.flush() {
            log::error!("{e}");
        }
    }
    async fn terminate(&mut self) {
        /* nothing to do */
    }

    /// Warp the pointer of `handle` to just inside the monitor that is
    /// outermost on `side`, using an absolute motion in the global logical
    /// layout. This makes re-entering a multi-monitor host land the cursor on
    /// the seam monitor instead of wherever it happened to be last. Requires
    /// the output geometry, which xdg_output provides; if it is not available
    /// yet the warp is skipped and the pointer resumes relatively.
    async fn warp_to_edge(
        &mut self,
        handle: EmulationHandle,
        side: PointerSide,
    ) -> Result<(), EmulationError> {
        const MARGIN: i32 = 12;
        let geoms: Vec<OutputGeom> = self
            .state
            .outputs
            .iter()
            .filter(|o| o.has_geom)
            .map(|o| o.geom.clone())
            .collect();
        let Some(((tx, ty), (min_x, min_y), (max_x, max_y))) = seam_target(&geoms, side, MARGIN)
        else {
            log::debug!("seam warp skipped: no output geometry known yet");
            return Ok(());
        };
        let width = (max_x - min_x).max(0) as u32;
        let height = (max_y - min_y).max(0) as u32;
        if width == 0 || height == 0 {
            return Ok(());
        }
        let x = (tx - min_x).clamp(0, width as i32) as u32;
        let y = (ty - min_y).clamp(0, height as i32) as u32;

        let now: u32 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u32;

        if let Some(virtual_input) = self.state.input_for_client.get(&handle) {
            log::info!("warping pointer to {side:?} edge at ({x},{y}) / {width}x{height}");
            virtual_input
                .pointer
                .motion_absolute(now, x, y, width, height);
            virtual_input.pointer.frame();
            if let Err(e) = self.queue.flush() {
                log::error!("failed to flush seam warp: {e}");
            }
        }
        Ok(())
    }
}

struct VirtualInput {
    pointer: Vp,
    keyboard: Vk,
    modifiers: Arc<Mutex<XMods>>,
}

impl VirtualInput {
    fn consume_event(&self, event: Event) -> Result<(), ()> {
        let now: u32 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u32;

        match event {
            Event::Pointer(e) => {
                match e {
                    PointerEvent::Motion { time, dx, dy } => self.pointer.motion(time, dx, dy),
                    PointerEvent::Button {
                        time,
                        button,
                        state,
                    } => {
                        let state: ButtonState = state.try_into()?;
                        self.pointer.button(time, button, state);
                    }
                    PointerEvent::Axis { time, axis, value } => {
                        let axis: Axis = (axis as u32).try_into()?;
                        self.pointer.axis(time, axis, value);
                        self.pointer.frame();
                    }
                    PointerEvent::AxisDiscrete120 { axis, value } => {
                        let axis: Axis = (axis as u32).try_into()?;
                        self.pointer
                            .axis_discrete(now, axis, value as f64 / 8., value / 120);
                        self.pointer.axis_source(AxisSource::Wheel);
                        self.pointer.frame();
                    }
                }
                self.pointer.frame();
            }
            Event::Keyboard(e) => match e {
                KeyboardEvent::Key { time, key, state } => {
                    self.keyboard.key(time, key, state as u32);
                    if let Ok(mut mods) = self.modifiers.lock() {
                        if mods.update_by_key_event(key, state) {
                            log::trace!("Key triggers modifier change: {mods:?}");
                            self.keyboard.modifiers(
                                mods.mask_pressed().bits(),
                                0,
                                mods.mask_locks().bits(),
                                0,
                            );
                        }
                    }
                }
                KeyboardEvent::Modifiers {
                    depressed: mods_depressed,
                    latched: mods_latched,
                    locked: mods_locked,
                    group,
                } => {
                    // Synchronize internal modifier state, assuming server is authoritative
                    if let Ok(mut mods) = self.modifiers.lock() {
                        mods.update_by_mods_event(e);
                    }
                    self.keyboard
                        .modifiers(mods_depressed, mods_latched, mods_locked, group);
                }
            },
        }
        Ok(())
    }
}

delegate_noop!(State: Vp);
delegate_noop!(State: Vk);
delegate_noop!(State: VpManager);
delegate_noop!(State: VkManager);
delegate_noop!(State: ZxdgOutputManagerV1);

impl Dispatch<WlRegistry, GlobalListContents> for State {
    fn event(
        state: &mut Self,
        _registry: &WlRegistry,
        event: wl_registry::Event,
        _: &GlobalListContents,
        _: &Connection,
        _: &QueueHandle<State>,
    ) {
        match event {
            wl_registry::Event::Global {
                name,
                interface,
                version,
            } => state.register_global(Global {
                name,
                interface,
                version,
            }),
            wl_registry::Event::GlobalRemove { name } => state.deregister_global(name),
            _ => {}
        }
    }
}

impl Dispatch<WlOutput, u32> for State {
    fn event(
        state: &mut Self,
        _: &WlOutput,
        event: wl_output::Event,
        name: &u32,
        _: &Connection,
        _: &QueueHandle<State>,
    ) {
        if let wl_output::Event::Done = event {
            state.update_output_info(*name);
        }
    }
}

impl Dispatch<ZxdgOutputV1, u32> for State {
    fn event(
        state: &mut Self,
        _: &ZxdgOutputV1,
        event: zxdg_output_v1::Event,
        name: &u32,
        _: &Connection,
        _: &QueueHandle<State>,
    ) {
        match event {
            zxdg_output_v1::Event::LogicalPosition { x, y } => {
                if let Some(o) = state.outputs.iter_mut().find(|o| o.global_name == *name) {
                    o.pending.position = (x, y);
                }
            }
            zxdg_output_v1::Event::LogicalSize { width, height } => {
                if let Some(o) = state.outputs.iter_mut().find(|o| o.global_name == *name) {
                    o.pending.size = (width, height);
                }
            }
            zxdg_output_v1::Event::Name { name: n } => {
                if let Some(o) = state.outputs.iter_mut().find(|o| o.global_name == *name) {
                    o.pending.name = n;
                }
            }
            zxdg_output_v1::Event::Done => {
                state.update_output_info(*name);
            }
            _ => {}
        }
    }
}

impl Dispatch<WlKeyboard, ()> for State {
    fn event(
        state: &mut Self,
        _: &WlKeyboard,
        event: <WlKeyboard as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        if let wl_keyboard::Event::Keymap { format, fd, size } = event {
            state.keymap = Some((u32::from(format), fd, size));
        }
    }
}

impl Dispatch<WlSeat, ()> for State {
    fn event(
        _: &mut Self,
        seat: &WlSeat,
        event: <WlSeat as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        qhandle: &QueueHandle<Self>,
    ) {
        if let wl_seat::Event::Capabilities {
            capabilities: WEnum::Value(capabilities),
        } = event
        {
            if capabilities.contains(wl_seat::Capability::Keyboard) {
                seat.get_keyboard(qhandle, ());
            }
        }
    }
}

// From X11/X.h
bitflags! {
    #[repr(C)]
    #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
    struct XMods: u32 {
        const ShiftMask = (1<<0);
        const LockMask = (1<<1);
        const ControlMask = (1<<2);
        const Mod1Mask = (1<<3);
        const Mod2Mask = (1<<4);
        const Mod3Mask = (1<<5);
        const Mod4Mask = (1<<6);
        const Mod5Mask = (1<<7);
    }
}

impl XMods {
    fn update_by_mods_event(&mut self, evt: KeyboardEvent) {
        if let KeyboardEvent::Modifiers {
            depressed, locked, ..
        } = evt
        {
            *self = XMods::from_bits_truncate(depressed) | XMods::from_bits_truncate(locked);
        }
    }

    fn update_by_key_event(&mut self, key: u32, state: u8) -> bool {
        if let Ok(key) = scancode::Linux::try_from(key) {
            log::trace!("Attempting to process modifier from: {key:#?}");
            let pressed_mask = match key {
                scancode::Linux::KeyLeftShift | scancode::Linux::KeyRightShift => XMods::ShiftMask,
                scancode::Linux::KeyLeftCtrl | scancode::Linux::KeyRightCtrl => XMods::ControlMask,
                scancode::Linux::KeyLeftAlt | scancode::Linux::KeyRightalt => XMods::Mod1Mask,
                scancode::Linux::KeyLeftMeta | scancode::Linux::KeyRightmeta => XMods::Mod4Mask,
                _ => XMods::empty(),
            };

            let locked_mask = match key {
                scancode::Linux::KeyCapsLock => XMods::LockMask,
                scancode::Linux::KeyNumlock => XMods::Mod2Mask,
                scancode::Linux::KeyScrollLock => XMods::Mod3Mask,
                _ => XMods::empty(),
            };

            // unchanged
            if pressed_mask.is_empty() && locked_mask.is_empty() {
                log::trace!("{key:#?} is not a modifier key");
                return false;
            }
            match state {
                1 => self.insert(pressed_mask),
                _ => {
                    self.remove(pressed_mask);
                    self.toggle(locked_mask);
                }
            }
            true
        } else {
            false
        }
    }

    fn mask_locks(&self) -> XMods {
        *self & (XMods::LockMask | XMods::Mod2Mask | XMods::Mod3Mask)
    }

    fn mask_pressed(&self) -> XMods {
        *self & (XMods::ShiftMask | XMods::ControlMask | XMods::Mod1Mask | XMods::Mod4Mask)
    }
}
