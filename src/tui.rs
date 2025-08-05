use ratatui::{
    DefaultTerminal, Frame,
    crossterm::{
        self,
        event::{KeyCode, KeyEvent, KeyEventKind},
    },
    style::Stylize,
    text::Line,
    widgets::Widget,
};

struct App {
    exit: bool,
}

pub fn run_tui_app() -> anyhow::Result<()> {
    let mut terminal = ratatui::init();

    let mut app = App { exit: false };

    app.run(&mut terminal)?;

    ratatui::restore();

    Ok(())
}

impl App {
    #[warn(clippy::single_match)]
    fn run(&mut self, terminal: &mut DefaultTerminal) -> anyhow::Result<()> {
        while !self.exit {
            match crossterm::event::read()? {
                crossterm::event::Event::Key(key_event) => self.handle_key_event(key_event)?,
                _ => {}
            }
            terminal.draw(|frame| self.draw(frame))?;
        }

        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) -> anyhow::Result<()> {
        if key_event.kind == KeyEventKind::Press && key_event.code == KeyCode::Char('q') {
            self.exit = true;
        }
        Ok(())
    }

    fn draw(&self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
    }
}

impl Widget for &App {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        Line::from("Process overview").bold().render(area, buf);
    }
}
