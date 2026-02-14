use std::collections::VecDeque;
use std::{io, time::Duration};

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    prelude::*,
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::interval;

#[derive(Parser, Debug)]
#[command(name = "stormdb-monitor", about = "Monitor TUI for StormDB")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 6379)]
    port: u16,
}

struct App {
    data: VecDeque<(f64, f64)>,
    window_size: usize,
    x_offset: f64,
}

impl App {
    fn new() -> Self {
        Self {
            data: VecDeque::with_capacity(100),
            window_size: 100,
            x_offset: 0.0,
        }
    }

    fn add_point(&mut self, y: f64) {
        self.x_offset += 1.0;
        if self.data.len() >= self.window_size {
            self.data.pop_front();
        }
        self.data.push_back((self.x_offset, y));
    }

    fn to_dataset(&self) -> Vec<(f64, f64)> {
        self.data.iter().cloned().collect()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);

    // Setup Terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // App state
    let mut app = App::new();
    let mut ticker = interval(Duration::from_secs(1));

    // Connection loop
    let mut stream = TcpStream::connect(&addr).await?;

    // UI Loop
    loop {
        // Draw
        terminal.draw(|f| ui(f, &app, &addr))?;

        // Handle Input (Non-blocking check)
        if event::poll(Duration::from_millis(0))?
            && let Event::Key(key) = event::read()?
                && key.code == KeyCode::Char('q') {
                    break;
                }

        // Update Data (Tick)
        tokio::select! {
            _ = ticker.tick() => {
                // Send DBSIZE (*1\r\n$6\r\nDBSIZE\r\n)
                let cmd = "*1\r\n$6\r\nDBSIZE\r\n";
                if stream.write_all(cmd.as_bytes()).await.is_err() {
                     // Try reconnect logic would go here
                     break;
                }

                // Read Response (Simple parser assuming integer response :123\r\n)
                let mut buf = [0u8; 128];
                match stream.read(&mut buf).await {
                    Ok(n) if n > 0 => {
                        let s = String::from_utf8_lossy(&buf[..n]);
                        if s.starts_with(':')
                             && let Ok(val) = s.trim()[1..].parse::<f64>() {
                                 app.add_point(val);
                             }
                    }
                    _ => break,
                }
            }
        }
    }

    // Restore Terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

fn ui(f: &mut Frame, app: &App, addr: &str) {
    let size = f.size();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(10), Constraint::Percentage(90)])
        .split(size);

    // Header
    let title = Paragraph::new(format!("StormDB Monitor - Connected to {}", addr))
        .block(Block::default().borders(Borders::ALL).title("Status"))
        .style(Style::default().fg(Color::Cyan));
    f.render_widget(title, chunks[0]);

    // Chart
    let data_points = app.to_dataset();
    let dataset = vec![
        Dataset::default()
            .name("Keys")
            .marker(symbols::Marker::Braille)
            .style(Style::default().fg(Color::Yellow))
            .graph_type(GraphType::Line)
            .data(&data_points),
    ];

    let x_labels = vec![
        Span::styled(
            format!("{:.0}", app.x_offset - app.window_size as f64),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{:.0}", app.x_offset),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ];

    let max_y = app.data.iter().map(|(_, y)| *y).fold(0.0, f64::max) + 10.0;

    let chart = Chart::new(dataset)
        .block(
            Block::default()
                .title("Keys over Time")
                .borders(Borders::ALL),
        )
        .x_axis(
            Axis::default()
                .title("Time (s)")
                .style(Style::default().fg(Color::Gray))
                .bounds([app.x_offset - app.window_size as f64, app.x_offset])
                .labels(x_labels),
        )
        .y_axis(
            Axis::default()
                .title("Count")
                .style(Style::default().fg(Color::Gray))
                .bounds([0.0, max_y])
                .labels(vec![
                    Span::raw("0"),
                    Span::styled(
                        format!("{:.0}", max_y),
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                ]),
        );

    f.render_widget(chart, chunks[1]);
}
